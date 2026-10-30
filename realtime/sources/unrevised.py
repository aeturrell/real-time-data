"""Vintages for series that ONS never revises, back-filled from today's figures.

Some series are never revised once published: the Retail Prices Index, for
example. For these, the value published today for any month is the value that
was first published, so only the publication date is missing. Each value is
dated at the end of the month after its reference period ends, which is the
latest it could have been published, with precision "upper_bound".

Only reference periods from the series' "unrevised_from" setting onwards are
used, and the build drops any of these rows dated after exact-date page
histories begin, so they only fill the years before late 2015.
"""

from collections.abc import Iterator

import pandas as pd
from loguru import logger

from realtime.cache import DownloadCache
from realtime.config import SeriesSpec
from realtime.http import DownloadError
from realtime.release_dates import Vintage
from realtime.settings import ONS_BASE_URL
from realtime.sources.series_json import (
    dataset_of_page,
    find_pages,
    observations_from_document,
)

SOURCE_NAME = "unrevised"
PRECISION = "upper_bound"
PUBLICATION_LAG_MONTHS = 1


class UnrevisedSource:
    """Back-fills first publications for series marked as never revised.

    Attributes:
        name: The source code written to the "source" column.
    """

    name = SOURCE_NAME

    def __init__(self, specs: tuple[SeriesSpec, ...], cache: DownloadCache) -> None:
        """Sets up the source.

        Args:
            specs: All configured series; only those with "unrevised_from" are used.
            cache: The download cache.
        """
        self._specs = tuple(spec for spec in specs if spec.unrevised_from)
        self._cache = cache

    def fetch(self) -> Iterator[pd.DataFrame]:
        """Yields the back-filled first publications of each unrevised series.

        Yields:
            One table per series.
        """
        for spec in self._specs:
            try:
                yield self._first_publications(spec)
            except (DownloadError, KeyError, ValueError) as error:
                logger.error(f"Skipping unrevised {spec.series_id}: {error}")

    def _first_publications(self, spec: SeriesSpec) -> pd.DataFrame:
        """Reads today's figures for a series and dates each one.

        Args:
            spec: The series.

        Returns:
            One row per reference period from "unrevised_from" onwards.

        Raises:
            ValueError: If the series has no page in its primary dataset.
        """
        page_uri = _primary_page(find_pages(self._cache, spec), spec)
        document, cached = self._cache.fetch_json_with_provenance(
            f"{ONS_BASE_URL}{page_uri}/data", immutable=False
        )
        placeholder = Vintage("current", pd.Timestamp.now().normalize(), PRECISION)
        rows = observations_from_document(
            document, cached, placeholder, spec, source=SOURCE_NAME
        )
        periods = rows["reference_period"].map(pd.Period)
        rows = rows.assign(vintage_date=periods.map(latest_publication_date))
        start = pd.Period(spec.unrevised_from).start_time
        kept = rows.loc[periods.map(lambda period: period.start_time >= start)]
        logger.info(
            f"Unrevised {spec.series_id}: {len(kept):,} first publications "
            f"from {spec.unrevised_from}"
        )
        return kept.reset_index(drop=True)


def latest_publication_date(period: pd.Period) -> pd.Timestamp:
    """Gives the latest date a period's figure can have first been published.

    Args:
        period: The reference period.

    Returns:
        The last day of the month after the period ends.
    """
    final_month = period.asfreq("M", how="end")
    return (final_month + PUBLICATION_LAG_MONTHS).end_time.normalize()


def _primary_page(page_uris: list[str], spec: SeriesSpec) -> str:
    """Picks the series' page in its primary dataset.

    Args:
        page_uris: Every page for the series.
        spec: The series.

    Returns:
        The page path.

    Raises:
        ValueError: If there is no page in the primary dataset.
    """
    matches = [
        page for page in page_uris if dataset_of_page(page) == spec.primary_dataset
    ]
    if not matches:
        raise ValueError(f"No {spec.primary_dataset} page for {spec.series_id}")
    return matches[0]
