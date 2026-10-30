"""Vintages from ONS revisions-triangle spreadsheets.

A revisions triangle has one column per reference period and one row per
publication month, so each row is a full snapshot of the series as published
that month. Triangles only give the publication month, so each vintage date is
set to the last day of that month (the latest it could have been published),
with precision "month".

Recent files often hold only the last few years of publication months, but every
dataset page keeps its past versions. Working back from the newest file, the
version that was live just before the earliest publication month covered so far
is downloaded next, until no older version adds anything. Where two files hold
the same publication month, the older file (closer to the original release) wins.
"""

import re
from collections.abc import Iterator
from dataclasses import dataclass

import pandas as pd
from loguru import logger

from realtime.cache import CachedFile, DownloadCache
from realtime.config import TriangleSpec
from realtime.periods import parse_period_label, period_to_string
from realtime.release_dates import london_date
from realtime.settings import ONS_BASE_URL
from realtime.sources.base import provenance_columns
from realtime.sources.dataset_files import (
    PREVIOUS_VERSION_MARKER,
    READ_ERRORS,
    download_spreadsheet,
    latest_edition,
    open_workbook,
    pick_sheet,
)
from realtime.store import collapse_to_changes

SOURCE_NAME = "triangle"
HEADER_PATTERN = re.compile(
    r"^(relating to period|time period row|index$)", re.IGNORECASE
)
MAX_FAILED_VERSIONS_IN_A_ROW = 3


@dataclass(frozen=True)
class TriangleLayout:
    """Where the parts of a triangle sit in a worksheet.

    Attributes:
        header_row: Row holding the reference periods.
        label_column: Column holding the publication months.
        period_columns: Column positions mapped to their reference periods.
        vintage_rows: Row positions mapped to their publication months.
    """

    header_row: int
    label_column: int
    period_columns: dict[int, pd.Period]
    vintage_rows: dict[int, pd.Period]


@dataclass(frozen=True)
class EditionVersion:
    """One version of a dataset edition page.

    Attributes:
        uri: The version's page path.
        replaced_at: When the version was replaced (London day); None if current.
    """

    uri: str
    replaced_at: pd.Timestamp | None


@dataclass(frozen=True)
class TriangleFile:
    """A downloaded triangle file and the rows read from it.

    Attributes:
        cached: The cached file.
        observations: Output of parse_triangle.
    """

    cached: CachedFile
    observations: pd.DataFrame

    @property
    def earliest_vintage(self) -> pd.Period:
        """The earliest publication month in the file."""
        return self.observations["vintage_month"].min()


class TriangleSource:
    """Collects each configured revisions triangle, from its newest and past files.

    Attributes:
        name: The source code written to the "source" column.
    """

    name = SOURCE_NAME

    def __init__(self, specs: tuple[TriangleSpec, ...], cache: DownloadCache) -> None:
        """Sets up the source.

        Args:
            specs: The triangles to collect.
            cache: The download cache.
        """
        self._specs = specs
        self._cache = cache

    def fetch(self) -> Iterator[pd.DataFrame]:
        """Yields the change-only history held in each triangle.

        Yields:
            One table per triangle.
        """
        for spec in self._specs:
            try:
                yield self._triangle_history(spec)
            except READ_ERRORS as error:
                logger.error(f"Skipping triangle {spec.series_id}: {error}")

    def _triangle_history(self, spec: TriangleSpec) -> pd.DataFrame:
        """Downloads and reads one triangle from as many files as needed.

        Args:
            spec: The triangle.

        Returns:
            Its change-only rows.
        """
        versions = self._edition_versions(
            latest_edition(self._cache, spec.dataset_path)
        )
        files = list(self._files_covering_history(spec, versions))
        rows = pd.concat(
            [
                _to_archive_rows(file.observations, spec, file.cached)
                for file in reversed(files)
            ],
            ignore_index=True,
        ).drop_duplicates(subset=["reference_period", "vintage_date"], keep="first")
        history = collapse_to_changes(rows)
        logger.info(
            f"Triangle {spec.series_id}: {rows['vintage_date'].nunique()} vintages "
            f"from {rows['vintage_date'].min():%Y-%m} in {len(files)} files, "
            f"{len(history):,} changes"
        )
        return history

    def _files_covering_history(
        self, spec: TriangleSpec, versions: list[EditionVersion]
    ) -> Iterator[TriangleFile]:
        """Yields the newest triangle file, then older ones that reach further back.

        Args:
            spec: The triangle.
            versions: The edition's versions, oldest first, current last.

        Yields:
            Triangle files, newest first.
        """
        index = len(versions) - 1
        newest = self._read_file(spec, versions[index])
        yield newest
        earliest = newest.earliest_vintage
        failures = 0
        while (
            failures < MAX_FAILED_VERSIONS_IN_A_ROW
            and (index := _version_live_before(versions, earliest, index)) is not None
        ):
            try:
                file = self._read_file(spec, versions[index])
            except READ_ERRORS as error:
                failures += 1
                logger.warning(f"Could not read {versions[index].uri}: {error}")
                continue
            failures = 0
            if file.earliest_vintage < earliest:
                earliest = file.earliest_vintage
                yield file

    def _read_file(self, spec: TriangleSpec, version: EditionVersion) -> TriangleFile:
        """Downloads and parses the triangle held by one version of an edition.

        Args:
            spec: The triangle.
            version: The edition version.

        Returns:
            The file and its rows.
        """
        cached = download_spreadsheet(self._cache, version.uri, spec.file_pattern)
        workbook = open_workbook(cached.content, cached.url, spec.member_pattern)
        sheet = pick_sheet(workbook, spec.sheet_pattern)
        return TriangleFile(cached, parse_triangle(sheet, spec.frequency))

    def _edition_versions(self, edition_uri: str) -> list[EditionVersion]:
        """Lists an edition's past versions and its current page.

        Args:
            edition_uri: The edition page path.

        Returns:
            The versions, oldest first, with the current page last.
        """
        edition = self._cache.fetch_json(
            f"{ONS_BASE_URL}{edition_uri}/data", immutable=False
        )
        past = sorted(edition.get("versions") or [], key=_version_number)
        return [
            *(
                EditionVersion(entry["uri"], london_date(entry.get("updateDate")))
                for entry in past
            ),
            EditionVersion(edition_uri, None),
        ]


def _version_live_before(
    versions: list[EditionVersion], earliest: pd.Period, before: int
) -> int | None:
    """Picks the next, older version to download.

    This is the version that was live at the end of the month before the earliest
    publication month covered so far, so its newest row joins on to what is held.

    Args:
        versions: The edition's versions, oldest first, current last.
        earliest: The earliest publication month covered so far.
        before: The position of the last version downloaded; the pick is older.

    Returns:
        The position of the version, or None if there is no older version.
    """
    month_end = (earliest - 1).end_time.normalize()
    live = next(
        (
            index
            for index, version in enumerate(versions)
            if version.replaced_at is None or version.replaced_at > month_end
        ),
        len(versions) - 1,
    )
    index = min(live, before - 1)
    return index if index >= 0 else None


def _version_number(entry: dict) -> int:
    """Gives the number of a past version from its path, such as ".../previous/v12".

    Args:
        entry: A version entry from an edition page's "versions" list.

    Returns:
        The version number.
    """
    return int(entry["uri"].rsplit(PREVIOUS_VERSION_MARKER, 1)[-1])


def parse_triangle(sheet: pd.DataFrame, frequency: str) -> pd.DataFrame:
    """Reads the body of a triangle worksheet into long form.

    Args:
        sheet: The worksheet, read with no header.
        frequency: The frequency of the reference periods: "M" or "Q".

    Returns:
        Columns "vintage_month" (monthly period), "reference_period" (period)
        and "value". When a publication month appears twice, the later row wins.

    Raises:
        ValueError: If no triangle layout can be found.
    """
    layout = find_layout(sheet, frequency)
    rows = [
        {
            "vintage_month": vintage_month,
            "reference_period": period,
            "value": sheet.iat[row, column],
        }
        for row, vintage_month in layout.vintage_rows.items()
        for column, period in layout.period_columns.items()
    ]
    long = pd.DataFrame(rows, columns=["vintage_month", "reference_period", "value"])
    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    long = long.dropna(subset=["value"])
    return long.drop_duplicates(
        subset=["vintage_month", "reference_period"], keep="last"
    )


def find_layout(sheet: pd.DataFrame, frequency: str) -> TriangleLayout:
    """Finds the header row and publication-month column of a triangle.

    Some workbooks hold a summary table above the triangle, so every header
    candidate is tried and the one with the most publication-month rows wins.

    Args:
        sheet: The worksheet, read with no header.
        frequency: The frequency of the reference periods.

    Returns:
        The layout.

    Raises:
        ValueError: If no header with publication months below it is found.
    """
    layouts = [
        _layout_at(sheet, row, column, frequency)
        for row, column in _header_cells(sheet)
    ]
    usable = [
        layout for layout in layouts if layout.vintage_rows and layout.period_columns
    ]
    if not usable:
        raise ValueError("No revisions triangle found in worksheet")
    return max(usable, key=lambda layout: len(layout.vintage_rows))


def _header_cells(sheet: pd.DataFrame) -> list[tuple[int, int]]:
    """Finds cells that start with "Relating to period", or similar, or are exactly "Index".

    Args:
        sheet: The worksheet.

    Returns:
        (row, column) positions.
    """
    return [
        (row, column)
        for row in range(sheet.shape[0])
        for column in range(sheet.shape[1])
        if isinstance(sheet.iat[row, column], str)
        and HEADER_PATTERN.match(sheet.iat[row, column].strip())
    ]


def _layout_at(
    sheet: pd.DataFrame, header_row: int, label_column: int, frequency: str
) -> TriangleLayout:
    """Reads the layout implied by a header cell.

    Args:
        sheet: The worksheet.
        header_row: Row of the header cell.
        label_column: Column of the header cell.
        frequency: The frequency of the reference periods.

    Returns:
        The layout (possibly with no periods or vintages if the guess was wrong).
    """
    period_columns = {
        column: period
        for column in range(label_column + 1, sheet.shape[1])
        if (period := parse_period_label(sheet.iat[header_row, column], frequency))
        is not None
    }
    vintage_rows = {
        row: vintage_month
        for row in range(header_row + 1, sheet.shape[0])
        if (vintage_month := _publication_month(sheet.iat[row, label_column]))
        is not None
    }
    return TriangleLayout(header_row, label_column, period_columns, vintage_rows)


def _publication_month(label: object) -> pd.Period | None:
    """Reads a row label as a publication month, rejecting summary rows.

    Args:
        label: The row label, for example a datetime or "May-92".

    Returns:
        The monthly period, or None.
    """
    if isinstance(label, str) and not any(character.isdigit() for character in label):
        return None
    return parse_period_label(label, "M")


def _to_archive_rows(
    observations: pd.DataFrame, spec: TriangleSpec, cached: CachedFile
) -> pd.DataFrame:
    """Adds the archive's descriptive and provenance columns to parsed triangle rows.

    Args:
        observations: Output of parse_triangle.
        spec: The triangle.
        cached: The cached file the triangle came from.

    Returns:
        Rows ready for the archive.
    """
    month_ends = {
        month: month.end_time.normalize()
        for month in observations["vintage_month"].unique()
    }
    return pd.DataFrame(
        {
            "reference_period": observations["reference_period"].map(period_to_string),
            "vintage_date": observations["vintage_month"].map(month_ends),
            "value": observations["value"],
        }
    ).assign(
        series_id=spec.series_id,
        dataset_id=spec.dataset_path.rstrip("/").rsplit("/", 1)[-1],
        title=spec.title,
        short_name=spec.short_name,
        measure=spec.measure,
        frequency=spec.frequency,
        reference_period_basis=spec.basis,
        vintage_date_precision="month",
        **provenance_columns(SOURCE_NAME, cached),
    )
