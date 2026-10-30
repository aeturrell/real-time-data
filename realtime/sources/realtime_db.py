"""Vintages from ONS real-time database spreadsheets for GDP and its components.

A real-time database has one column per publication and one row per quarter, so
each column is a full snapshot of the series as published at that time. Some go
back to 1961. Columns are labelled in one of three ways, each of which gives the
publication month:

- by publication month, for example "Sep-61 [1954 prices]" or "Aug- 26 1st";
- by the latest quarter and the estimate, for example "1998 Q3 M2", where the
  estimate gives the number of months after the quarter ends (preliminary "M1"
  one, second "M2" two, first quarterly "1st" two, national accounts "QNA" three);
- by quarter and estimate in words, for example "Jan to Mar 2011 (P)", where
  provisional "(P)" comes two months after the quarter ends and revised "(R)" three.

Only the publication month is known, so each vintage date is the last day of that
month, with precision "month".

A few labels in the ONS files are typing mistakes ("Mar-62" between "Feb-69" and
"Apr-69", for example). A label that does not fall between its neighbours is
replaced by the month before the next column's, the latest it could have been
published, and the repair is logged.
"""

import re
from collections.abc import Iterator

import pandas as pd
from loguru import logger

from realtime.cache import CachedFile, DownloadCache
from realtime.config import RealtimeDbSpec
from realtime.periods import parse_period_label, period_to_string
from realtime.sources.base import provenance_columns
from realtime.sources.dataset_files import (
    READ_ERRORS,
    download_spreadsheet,
    latest_edition,
    open_workbook,
    pick_sheets,
)
from realtime.store import collapse_to_changes

SOURCE_NAME = "realtime_db"
FREQUENCY = "Q"
MAX_HEADER_ROWS = 20
MAX_LABEL_COLUMNS = 3
MIN_VINTAGE_COLUMNS = 2
TWO_DIGIT_YEAR_PIVOT = 50
MONTHS_AFTER_QUARTER = {"M1": 1, "M2": 2, "M3": 3, "1ST": 2, "QNA": 3, "P": 2, "R": 3}
MONTH_NUMBERS = {
    name: number
    for number, name in enumerate(
        ["JAN", "FEB", "MAR", "APR", "MAY", "JUN"]
        + ["JUL", "AUG", "SEP", "OCT", "NOV", "DEC"],
        start=1,
    )
}
PRICE_BASE_PATTERN = re.compile(r"\[[^\]]*\]")
MONTH_YEAR_PATTERN = re.compile(r"^([A-Z]{3})[A-Z]*\s*-\s*(\d{4}|\d{2})\b")
YEAR_QUARTER_PATTERN = re.compile(r"\b(\d{4})\s*Q([1-4])\b")
QUARTER_YEAR_PATTERN = re.compile(r"\bQ([1-4])\s+(\d{4})\b")
ESTIMATE_PATTERN = re.compile(r"\b(M1|M2|M3|QNA|1ST)\s*$")
MONTH_RANGE_PATTERN = re.compile(r"^([A-Z]{3}) TO ([A-Z]{3}) (\d{4}) \((P|R)\)")


class RealtimeDbSource:
    """Collects every vintage held in each configured ONS real-time database.

    Attributes:
        name: The source code written to the "source" column.
    """

    name = SOURCE_NAME

    def __init__(self, specs: tuple[RealtimeDbSpec, ...], cache: DownloadCache) -> None:
        """Sets up the source.

        Args:
            specs: The databases to collect.
            cache: The download cache.
        """
        self._specs = specs
        self._cache = cache

    def fetch(self) -> Iterator[pd.DataFrame]:
        """Yields the change-only history held in each database.

        Yields:
            One table per database.
        """
        for spec in self._specs:
            try:
                yield self._database_history(spec)
            except READ_ERRORS as error:
                logger.error(f"Skipping real-time database {spec.series_id}: {error}")

    def _database_history(self, spec: RealtimeDbSpec) -> pd.DataFrame:
        """Downloads and reads one database.

        Args:
            spec: The database.

        Returns:
            Its change-only rows.
        """
        edition = latest_edition(self._cache, spec.dataset_path)
        cached = download_spreadsheet(self._cache, edition, spec.file_pattern)
        workbook = open_workbook(cached.content, cached.url, None)
        observations = pd.concat(
            [
                read_database(sheet)
                for sheet in pick_sheets(workbook, spec.sheet_pattern)
            ],
            ignore_index=True,
        ).drop_duplicates(subset=["vintage_month", "reference_period"], keep="last")
        history = collapse_to_changes(_to_archive_rows(observations, spec, cached))
        logger.info(
            f"Real-time database {spec.series_id}: "
            f"{observations['vintage_month'].nunique()} vintages from "
            f"{observations['vintage_month'].min()}, {len(history):,} changes"
        )
        return history


def vintage_month_of_label(label: object) -> pd.Period | None:
    """Reads the publication month from a real-time database column label.

    Args:
        label: For example "Sep-61 [1954 prices]", "1998 Q3 M2" or
            "Jan to Mar 2011 (P) 2007 prices".

    Returns:
        The publication month, or None if the label is not a vintage.
    """
    if not isinstance(label, str):
        return None
    text = " ".join(PRICE_BASE_PATTERN.sub(" ", label).upper().split())
    if match := MONTH_YEAR_PATTERN.match(text):
        return _month(match[1], _full_year(match[2]))
    if match := MONTH_RANGE_PATTERN.match(text):
        quarter_end = _month(match[2], int(match[3]))
        return quarter_end + MONTHS_AFTER_QUARTER[match[4]] if quarter_end else None
    quarter = _quarter(text)
    estimate = ESTIMATE_PATTERN.search(text)
    if quarter is None or estimate is None:
        return None
    return quarter.asfreq("M", how="end") + MONTHS_AFTER_QUARTER[estimate[1]]


def read_database(sheet: pd.DataFrame) -> pd.DataFrame:
    """Reads one real-time database worksheet into long form.

    Args:
        sheet: The worksheet, read with no header.

    Returns:
        Columns "vintage_month", "reference_period" and "value". When two
        columns give the same publication month, the later column wins.

    Raises:
        ValueError: If no row of publication labels is found.
    """
    header_row, labelled_columns = _find_header(sheet)
    vintage_columns = repair_out_of_order(labelled_columns)
    label_column = _find_label_column(sheet, header_row)
    period_rows = {
        row: period
        for row in range(header_row + 1, sheet.shape[0])
        if (period := _reference_period(sheet.iat[row, label_column])) is not None
    }
    rows = [
        {
            "vintage_month": vintage_month,
            "reference_period": period,
            "value": sheet.iat[row, column],
        }
        for column, vintage_month in vintage_columns.items()
        for row, period in period_rows.items()
    ]
    long = pd.DataFrame(rows, columns=["vintage_month", "reference_period", "value"])
    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    return long.dropna(subset=["value"]).drop_duplicates(
        subset=["vintage_month", "reference_period"], keep="last"
    )


def _find_header(sheet: pd.DataFrame) -> tuple[int, dict[int, pd.Period]]:
    """Finds the row of publication labels: the one where most cells read as vintages.

    Args:
        sheet: The worksheet.

    Returns:
        The header row and its column positions mapped to publication months.

    Raises:
        ValueError: If no row has enough publication labels.
    """
    candidates = [
        (
            row,
            {
                column: month
                for column in range(sheet.shape[1])
                if (month := vintage_month_of_label(sheet.iat[row, column])) is not None
            },
        )
        for row in range(min(MAX_HEADER_ROWS, sheet.shape[0]))
    ]
    header_row, columns = max(candidates, key=lambda candidate: len(candidate[1]))
    if len(columns) < MIN_VINTAGE_COLUMNS:
        raise ValueError("No row of publication labels found in worksheet")
    return header_row, columns


def repair_out_of_order(columns: dict[int, pd.Period]) -> dict[int, pd.Period]:
    """Replaces publication months that do not follow on from their neighbours.

    Columns are checked in order against the (already repaired) previous column. A
    month is kept if it is later than the previous one and no later than the next
    (a next month that is itself earlier than the previous one is ignored as the
    mistake). Otherwise it becomes the month before the next column's, or the
    previous column's month if that is no earlier.

    Args:
        columns: Column positions mapped to the publication months their labels give.

    Returns:
        The columns with out-of-order months replaced.
    """
    positions = sorted(columns)
    repaired = {positions[0]: columns[positions[0]]} if positions else {}
    for index, column in enumerate(positions[1:], start=1):
        previous = repaired[positions[index - 1]]
        month = columns[column]
        following = (
            columns[positions[index + 1]] if index + 1 < len(positions) else None
        )
        if _follows_on(previous, month, following):
            repaired[column] = month
            continue
        repaired[column] = (
            previous if following is None else max(following - 1, previous)
        )
        logger.warning(
            f"Column {column}: publication month {month} is out of order after "
            f"{previous}; using {repaired[column]}"
        )
    return repaired


def _follows_on(
    previous: pd.Period, month: pd.Period, following: pd.Period | None
) -> bool:
    """Checks whether a publication month fits between its neighbours.

    Args:
        previous: The previous column's (repaired) month.
        month: This column's month.
        following: The next column's month, if there is one.

    Returns:
        True if the month is later than the previous one and no later than the next,
        or the next one is itself out of order.
    """
    if month <= previous:
        return False
    return following is None or following <= previous or month <= following


def _find_label_column(sheet: pd.DataFrame, header_row: int) -> int:
    """Finds the column holding the reference quarters.

    Args:
        sheet: The worksheet.
        header_row: The row of publication labels.

    Returns:
        The column, among the first few, with the most quarter labels below the header.
    """
    counts = {
        column: sum(
            _reference_period(sheet.iat[row, column]) is not None
            for row in range(header_row + 1, sheet.shape[0])
        )
        for column in range(min(MAX_LABEL_COLUMNS, sheet.shape[1]))
    }
    return max(counts, key=counts.get)


def _reference_period(label: object) -> pd.Period | None:
    """Reads a row label such as "Q1 1955" or "1997 Q1 " as a quarter.

    Args:
        label: The row label.

    Returns:
        The quarter, or None.
    """
    return parse_period_label(label, FREQUENCY) if isinstance(label, str) else None


def _quarter(text: str) -> pd.Period | None:
    """Finds a quarter written as "1998 Q3" or "Q3 1998" in a label.

    Args:
        text: The upper-case label.

    Returns:
        The quarter, or None.
    """
    if match := YEAR_QUARTER_PATTERN.search(text):
        return pd.Period(year=int(match[1]), quarter=int(match[2]), freq="Q-DEC")
    if match := QUARTER_YEAR_PATTERN.search(text):
        return pd.Period(year=int(match[2]), quarter=int(match[1]), freq="Q-DEC")
    return None


def _month(name: str, year: int) -> pd.Period | None:
    """Builds a monthly period from a three-letter month name and a year.

    Args:
        name: For example "SEP".
        year: For example 1961.

    Returns:
        The month, or None if the name is not a month.
    """
    number = MONTH_NUMBERS.get(name)
    return pd.Period(year=year, month=number, freq="M") if number else None


def _full_year(digits: str) -> int:
    """Turns a two- or four-digit year into a full year.

    Args:
        digits: For example "61", "26" or "2026".

    Returns:
        The year: two-digit years from 50 are 19xx, below 50 are 20xx.
    """
    year = int(digits)
    if len(digits) == len("2026"):
        return year
    return year + (1900 if year >= TWO_DIGIT_YEAR_PIVOT else 2000)


def _to_archive_rows(
    observations: pd.DataFrame, spec: RealtimeDbSpec, cached: CachedFile
) -> pd.DataFrame:
    """Adds the archive's descriptive and provenance columns to parsed rows.

    Args:
        observations: Output of read_database.
        spec: The database.
        cached: The cached file the database came from.

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
        frequency=FREQUENCY,
        reference_period_basis="period",
        vintage_date_precision="month",
        **provenance_columns(SOURCE_NAME, cached),
    )
