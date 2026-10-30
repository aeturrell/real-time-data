"""Vintages from the past versions of individual ONS time-series pages (late 2015 onwards).

Every ONS time-series page (for example /economy/grossdomesticproductgdp/timeseries/abmi/qna)
keeps every past version at /previous/vN. The same series often appears on
several dataset pages (GDP appears in PGDP, PN2, QNA, UKEA and BB); all of them
are collected, and change-only storage removes the overlap.
"""

from collections.abc import Iterator

import pandas as pd
from loguru import logger

from realtime.cache import CachedFile, DownloadCache
from realtime.config import SeriesSpec
from realtime.http import DownloadError
from realtime.periods import is_three_month_range, parse_period_label, period_to_string
from realtime.release_dates import PageVersion, Vintage, assign_vintages, london_date
from realtime.settings import ONS_BASE_URL, ONS_SEARCH_API_URL
from realtime.sources.base import provenance_columns
from realtime.store import collapse_to_changes

SOURCE_NAME = "series_json"
SEARCH_RESULT_LIMIT = 50
OBSERVATION_GROUPS = {"years": "A", "quarters": "Q", "months": "M"}
ONE_MONTH = 1


class SeriesJsonSource:
    """Collects every past version of every ONS page for a list of series.

    Attributes:
        name: The source code written to the "source" column.
    """

    name = SOURCE_NAME

    def __init__(self, specs: tuple[SeriesSpec, ...], cache: DownloadCache) -> None:
        """Sets up the source.

        Args:
            specs: The series to collect.
            cache: The download cache.
        """
        self._specs = specs
        self._cache = cache

    def fetch(self) -> Iterator[pd.DataFrame]:
        """Yields the change-only history of each page of each series.

        Yields:
            One table per series page.
        """
        for spec in self._specs:
            try:
                page_uris = find_pages(self._cache, spec)
            except DownloadError as error:
                logger.error(f"Could not find pages for {spec.series_id}: {error}")
                continue
            for page_uri in page_uris:
                try:
                    yield self._page_history(spec, page_uri)
                except (DownloadError, KeyError, ValueError) as error:
                    logger.error(f"Skipping {page_uri}: {error}")

    def _page_history(self, spec: SeriesSpec, page_uri: str) -> pd.DataFrame:
        """Downloads every version of one page and returns its change-only history.

        Args:
            spec: The series.
            page_uri: The page path.

        Returns:
            The change-only rows for this page.
        """
        current, current_file = self._cache.fetch_json_with_provenance(
            f"{ONS_BASE_URL}{page_uri}/data", immutable=False
        )
        entries = sorted(current.get("versions") or [], key=_version_number)
        past = [
            self._cache.fetch_json_with_provenance(
                f"{ONS_BASE_URL}{entry['uri']}/data", immutable=True
            )
            for entry in entries
        ]
        documents = [*past, (current, current_file)]
        versions = [
            PageVersion(
                label=entry["label"],
                release_date=_release_date(document),
                replaced_at=london_date(entry.get("updateDate")),
                latest_period_end=latest_period_end(document),
            )
            for entry, (document, _) in zip(entries, past, strict=True)
        ]
        versions.append(
            PageVersion(
                "current", _release_date(current), None, latest_period_end(current)
            )
        )
        vintages = assign_vintages(versions)
        frames = [
            observations_from_document(document, cached, vintage, spec)
            for (document, cached), vintage in zip(documents, vintages, strict=True)
        ]
        history = collapse_to_changes(pd.concat(frames, ignore_index=True))
        logger.info(f"{page_uri}: {len(documents)} versions, {len(history):,} changes")
        return history


def find_pages(cache: DownloadCache, spec: SeriesSpec) -> list[str]:
    """Finds every ONS page for a series using the ONS search API.

    Args:
        cache: The download cache.
        spec: The series.

    Returns:
        Page paths, for example "/economy/.../timeseries/abmi/qna", sorted,
        without the series' excluded datasets.
    """
    url = (
        f"{ONS_SEARCH_API_URL}?content_type=timeseries"
        f"&cdids={spec.series_id}&limit={SEARCH_RESULT_LIMIT}"
    )
    results = cache.fetch_json(url, immutable=False)
    marker = f"/timeseries/{spec.series_id.lower()}/"
    pages = sorted(
        {item["uri"] for item in results.get("items", []) if marker in item["uri"]}
    )
    kept = [
        page for page in pages if dataset_of_page(page) not in spec.exclude_datasets
    ]
    logger.info(
        f"{spec.series_id}: {len(kept)} pages ({', '.join(map(dataset_of_page, kept))})"
    )
    return kept


def observations_from_document(
    document: dict,
    cached: CachedFile,
    vintage: Vintage,
    spec: SeriesSpec,
    source: str = SOURCE_NAME,
) -> pd.DataFrame:
    """Turns one version of a time-series page into rows of the archive.

    Args:
        document: The page's JSON.
        cached: The cached file the JSON came from.
        vintage: The vintage date given to this version.
        spec: The series.
        source: The source code to record.

    Returns:
        One row per published observation, with reference periods as strings.
    """
    raw = pd.DataFrame(
        [
            {
                "frequency": frequency,
                "date": item.get("date"),
                "label": item.get("label"),
                "value": item.get("value"),
            }
            for group, frequency in OBSERVATION_GROUPS.items()
            for item in document.get(group) or []
        ],
        columns=["frequency", "date", "label", "value"],
    )
    raw["value"] = pd.to_numeric(raw["value"], errors="coerce")
    raw = raw.dropna(subset=["value"])
    periods = [
        _reference_period(row.date, row.label, row.frequency)
        for row in raw.itertuples()
    ]
    raw["reference_period"] = [
        period_to_string(period) if period else None for period, _ in periods
    ]
    raw["reference_period_basis"] = [basis for _, basis in periods]
    raw = raw.dropna(subset=["reference_period"])
    description = document.get("description", {})
    page_uri = document.get("uri", "")
    return raw.drop(columns=["date", "label"]).assign(
        series_id=spec.series_id,
        dataset_id=(description.get("datasetId") or dataset_of_page(page_uri)).upper(),
        title=(description.get("title") or "").strip(),
        short_name=spec.short_name,
        measure=spec.measure,
        vintage_date=vintage.vintage_date,
        vintage_date_precision=vintage.precision,
        **provenance_columns(source, cached),
    )


def latest_period_end(document: dict) -> pd.Timestamp | None:
    """Gives the last day of the latest period a version of a page holds.

    Only the finest frequency on the page is used, because coarser figures can
    appear before their period ends (workforce jobs give each year's June value
    as the annual figure, for example).

    Args:
        document: The page's JSON.

    Returns:
        Midnight on the last day of the latest period with a value, or None if
        there are none.
    """
    for group, frequency in reversed(OBSERVATION_GROUPS.items()):
        periods = [
            _reference_period(item.get("date"), item.get("label"), frequency)[0]
            for item in document.get(group) or []
            if item.get("value") not in (None, "")
        ]
        known = [period for period in periods if period is not None]
        if known:
            return max(period.end_time.normalize() for period in known)
    return None


def dataset_of_page(page_uri: str) -> str:
    """Gives the dataset code at the end of a time-series page path.

    Args:
        page_uri: For example "/economy/.../timeseries/abmi/qna".

    Returns:
        For example "QNA".
    """
    return page_uri.rstrip("/").rsplit("/", 1)[-1].upper()


def _reference_period(
    date: str, label: str | None, frequency: str
) -> tuple[pd.Period | None, str]:
    """Reads one observation's reference period and its basis.

    Labour market pages give rolling three-month figures dated by their middle
    month (date "2026 JUL", label "2026 JUN-AUG"). These are stored against the
    month the window ends, with basis "3m_ending".

    Args:
        date: The observation's "date" field.
        label: The observation's "label" field.
        frequency: "A", "Q" or "M".

    Returns:
        The period (or None if unreadable) and its basis.
    """
    period = parse_period_label(date, frequency)
    if period is not None and frequency == "M" and is_three_month_range(label):
        return period + ONE_MONTH, "3m_ending"
    return period, "period"


def _release_date(document: dict) -> pd.Timestamp | None:
    """Reads the release date a page version gives for itself.

    Args:
        document: The page's JSON.

    Returns:
        The London calendar date, or None.
    """
    return london_date(document.get("description", {}).get("releaseDate"))


def _version_number(entry: dict) -> int:
    """Gives the number of a version entry such as {"label": "v12"}.

    Args:
        entry: A version entry from a page's "versions" list.

    Returns:
        The version number.
    """
    return int(entry["label"].lstrip("v"))
