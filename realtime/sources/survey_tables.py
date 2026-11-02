"""Helpers shared by the survey sources: reading worksheets and building archive rows."""

import io
import re
import zipfile
from collections.abc import Iterable
from dataclasses import dataclass

import openpyxl
import pandas as pd
from openpyxl.utils.exceptions import InvalidFileException
from openpyxl.workbook.workbook import Workbook

from realtime.cache import CachedFile
from realtime.config import SurveySpec
from realtime.http import DownloadError
from realtime.sources.base import provenance_columns

READ_ERRORS = (
    DownloadError,
    KeyError,
    ValueError,
    zipfile.BadZipFile,
    InvalidFileException,
)
MIN_CELLS_IN_HEADER_ROW = 2
HEADER_SEPARATOR = " | "
Row = tuple[object, ...]


@dataclass(frozen=True)
class SurveyTable:
    """A survey worksheet whose rows are labelled by date in the first column.

    Attributes:
        columns: Each column's position mapped to its full heading, with the
            levels of a multi-row heading joined together.
        rows: The rows holding data, in the order they appear.
    """

    columns: dict[int, str]
    rows: tuple[Row, ...]


def clean_text(value: object) -> str:
    """Tidies a spreadsheet label by trimming it and collapsing repeated spaces.

    Args:
        value: The cell's contents.

    Returns:
        The tidied text, empty for a blank cell.
    """
    return "" if value is None else " ".join(str(value).split())


def open_workbook(cached: CachedFile) -> Workbook:
    """Opens a downloaded spreadsheet for reading.

    Args:
        cached: The downloaded file.

    Returns:
        The workbook, showing the values its formulas last calculated.
    """
    return openpyxl.load_workbook(
        io.BytesIO(cached.content), read_only=True, data_only=True
    )


def find_sheet(workbook: Workbook, pattern: str) -> str | None:
    """Finds the first worksheet whose tidied name matches a regular expression.

    Args:
        workbook: The workbook.
        pattern: The regular expression, ignoring case.

    Returns:
        The worksheet's exact name, or None if the workbook has no such sheet.
    """
    matches = (
        name
        for name in workbook.sheetnames
        if re.search(pattern, clean_text(name), re.IGNORECASE)
    )
    return next(matches, None)


def read_rows(workbook: Workbook, sheet_name: str) -> list[Row]:
    """Reads every row of a worksheet.

    Args:
        workbook: The workbook.
        sheet_name: The worksheet's exact name.

    Returns:
        The rows as tuples of cell values.
    """
    return list(workbook[sheet_name].iter_rows(values_only=True))


def to_number(value: object) -> float | None:
    """Reads a cell as a number, treating suppressed or empty markers as missing.

    Args:
        value: The cell's contents, such as 0.25, "[c]" or ".".

    Returns:
        The number, or None if the cell does not hold one.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    try:
        return float(clean_text(value))
    except ValueError:
        return None


def read_survey_table(rows: list[Row], is_date_label: object) -> SurveyTable:
    """Splits a worksheet into its headings and the rows labelled by date.

    Headings can span several rows, with a group heading written once above the
    columns it covers. Each group heading is carried across to the right. The
    bottom row names the columns themselves, so a blank there means no column.
    Rows holding a single cell are ignored because they are titles or notes.

    Args:
        rows: The worksheet's rows.
        is_date_label: A function saying whether a first-column cell is a date.

    Returns:
        The table.

    Raises:
        ValueError: If no row is labelled by a date.
    """
    dated = [index for index, row in enumerate(rows) if row and is_date_label(row[0])]
    if not dated:
        raise ValueError("no rows labelled by a date")
    header_rows = rows[: dated[0]]
    width = max(len(row) for row in rows)
    return SurveyTable(
        columns=_column_headings(header_rows, width),
        rows=tuple(rows[index] for index in dated),
    )


def find_column(
    columns: dict[int, str], answer: str | None, terms: Iterable[str]
) -> int | None:
    """Finds the column whose heading matches a series' description.

    Args:
        columns: Column positions mapped to full headings.
        answer: Text the final level of the heading must equal, if given.
        terms: Phrases the heading must all contain.

    Returns:
        The column's position, or None if no column matches.

    Raises:
        ValueError: If more than one column matches, since picking one would be
            a guess.
    """
    matches = [
        position
        for position, heading in columns.items()
        if _heading_matches(heading, answer, terms)
    ]
    if len(matches) > 1:
        found = [columns[position] for position in matches]
        raise ValueError(f"{len(matches)} columns match {answer} {terms}: {found}")
    return matches[0] if matches else None


def archive_rows(
    spec: SurveySpec,
    observations: pd.DataFrame,
    frequency: str,
    route: str,
    source: str,
    cached: CachedFile,
) -> pd.DataFrame:
    """Adds the archive's descriptive and provenance columns to a survey's values.

    Args:
        spec: The series.
        observations: Columns "reference_period", "vintage_date", "value" and
            "vintage_date_precision".
        frequency: The series' frequency code.
        route: The route code.
        source: The organisation that published the values.
        cached: The file the values came from.

    Returns:
        Rows ready for the archive.
    """
    return observations.assign(
        series_id=spec.series_id,
        dataset_id=spec.survey,
        title=spec.title,
        short_name=spec.short_name,
        measure=spec.measure,
        frequency=frequency,
        reference_period_basis=spec.basis,
        **provenance_columns(route, cached, source),
    )


def _column_headings(header_rows: list[Row], width: int) -> dict[int, str]:
    """Joins the levels of a multi-row heading for each column.

    Args:
        header_rows: The rows above the data.
        width: The number of columns.

    Returns:
        Column positions mapped to full headings, for columns that have one.
    """
    heading_rows = [
        row
        for row in header_rows
        if sum(bool(clean_text(cell)) for cell in row) >= MIN_CELLS_IN_HEADER_ROW
    ]
    if not heading_rows:
        return {}
    groups = [_carried_across(row, width) for row in heading_rows[:-1]]
    names = heading_rows[-1]
    return {
        position: HEADER_SEPARATOR.join(
            [*(group[position] for group in groups if group[position]), name]
        )
        for position, cell in enumerate(names)
        if (name := clean_text(cell))
    }


def _carried_across(row: Row, width: int) -> list[str]:
    """Copies each heading in a row into the blank cells to its right.

    Args:
        row: A heading row.
        width: The number of columns.

    Returns:
        One label per column.
    """
    labels: list[str] = []
    current = ""
    for position in range(width):
        cell = clean_text(row[position]) if position < len(row) else ""
        current = cell or current
        labels.append(current)
    return labels


def _heading_matches(heading: str, answer: str | None, terms: Iterable[str]) -> bool:
    """Checks a column heading against a series' description.

    Args:
        heading: The full heading.
        answer: Text the final level of the heading must equal, if given.
        terms: Phrases the heading must all contain.

    Returns:
        True if the heading matches, ignoring case.
    """
    lowered = heading.lower()
    final_level = lowered.rsplit(HEADER_SEPARATOR, 1)[-1]
    if answer and final_level != answer.lower():
        return False
    return all(term.lower() in lowered for term in terms)
