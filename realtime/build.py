"""Building the archive: run every source, merge, collapse to changes and write."""

import pandas as pd
from loguru import logger

from realtime.cache import DownloadCache
from realtime.config import ArchiveConfig
from realtime.settings import LATE_FIRST_APPEARANCES_PATH
from realtime.sources.base import SOURCE_PRIORITY, Source
from realtime.sources.realtime_db import RealtimeDbSource
from realtime.sources.series_json import SeriesJsonSource
from realtime.sources.triangle import TriangleSource
from realtime.sources.unrevised import UnrevisedSource
from realtime.store import (
    RANK_COLUMN,
    SOURCE_RANK_COLUMN,
    collapse_to_changes,
    label_release_types,
    prefer_best_sources,
)
from realtime.validate import late_first_appearances

SOURCE_TYPES = {
    SeriesJsonSource.name: lambda config, cache: SeriesJsonSource(config.series, cache),
    TriangleSource.name: lambda config, cache: TriangleSource(config.triangles, cache),
    UnrevisedSource.name: lambda config, cache: UnrevisedSource(config.series, cache),
    RealtimeDbSource.name: lambda config, cache: RealtimeDbSource(
        config.realtime_dbs, cache
    ),
}
NOT_PRIMARY_PENALTY = 1
RANK_STEP = 2


def create_sources(
    config: ArchiveConfig, cache: DownloadCache, only: list[str] | None = None
) -> list[Source]:
    """Creates the sources to run.

    Args:
        config: What to collect.
        cache: The download cache.
        only: Optional list of source names to restrict the run to.

    Returns:
        The sources, in a fixed order.
    """
    names = [name for name in SOURCE_TYPES if only is None or name in only]
    return [SOURCE_TYPES[name](config, cache) for name in names]


def build_archive(
    config: ArchiveConfig, cache: DownloadCache, only: list[str] | None = None
) -> pd.DataFrame:
    """Runs the sources and merges their output into one change-only table.

    Args:
        config: What to collect.
        cache: The download cache.
        only: Optional list of source names to restrict the run to.

    Returns:
        The change-only table, with reference periods as strings.

    Raises:
        RuntimeError: If no source produced any data.
    """
    frames = [
        frame
        for source in create_sources(config, cache, only)
        for frame in source.fetch()
    ]
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        raise RuntimeError("No source produced any data")
    observations = add_rank(
        pd.concat(frames, ignore_index=True), config.primary_datasets()
    )
    changes = label_release_types(
        collapse_to_changes(prefer_best_sources(observations))
    )
    logger.info(
        f"Merged {len(observations):,} rows into {len(changes):,} change-only rows"
    )
    record_late_first_appearances(observations, changes)
    return changes.drop(columns=[RANK_COLUMN, SOURCE_RANK_COLUMN])


def record_late_first_appearances(
    observations: pd.DataFrame, changes: pd.DataFrame
) -> None:
    """Saves, and warns about, periods first shown later than some source has them.

    Args:
        observations: Every source's rows, before weaker sources are set aside.
        changes: The merged change-only rows.
    """
    late = late_first_appearances(observations, changes)
    late.to_csv(LATE_FIRST_APPEARANCES_PATH, index=False)
    if not late.empty:
        logger.warning(
            f"{len(late)} periods first appear later than in some source; "
            f"see {LATE_FIRST_APPEARANCES_PATH}"
        )


def add_rank(
    observations: pd.DataFrame, primary_datasets: dict[str, str]
) -> pd.DataFrame:
    """Ranks each row by how much its source is trusted (lower is better).

    "_source_rank" ranks the source type by SOURCE_PRIORITY. "_rank" refines it:
    within a source type, a series' primary dataset beats its other datasets.

    Args:
        observations: Rows from every source.
        primary_datasets: Series ID to primary dataset ID.

    Returns:
        The rows with "_source_rank" and "_rank" columns.
    """
    source_rank = observations["source"].astype(str).map(SOURCE_PRIORITY)
    primary = observations["series_id"].astype(str).map(primary_datasets)
    is_primary = observations["dataset_id"].astype(str) == primary
    penalty = (~is_primary).astype(int) * NOT_PRIMARY_PENALTY
    return observations.assign(
        **{
            SOURCE_RANK_COLUMN: source_rank,
            RANK_COLUMN: source_rank * RANK_STEP + penalty,
        }
    )
