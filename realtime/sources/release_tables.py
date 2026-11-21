"""Reading series out of the reference tables that ONS attached to each release.

Before 2016 ONS published each release with a set of spreadsheets ("reference
tables"). Every column of figures in them is headed by the four-character code
(CDID) of the series it holds, and every row is labelled with its period down
the left-hand side, in one of many styles: "2013 Jan", "Mar-May 2013", "Q1
2013", a date, or a year on one row followed by month names on the rows below.

A column is read downwards from its code until the table ends: at a row of
text after the figures have begun (such as "Change on year" or a footnote), or
at the next row of codes. Some sheets repeat the same codes above blocks of
percentage changes; a block whose first heading mentions a change is skipped,
so that only levels are taken.
"""

import io
import re
import zipfile
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from itertools import pairwise
from numbers import Real

import pandas as pd

from realtime.periods import period_to_string

CODE_PATTERN = re.compile(r"^(?=[A-Z0-9]*[A-Z])[A-Z0-9]{4}$")
CHANGE_HEADING_PATTERN = re.compile(r"CHANGE|GROWTH|%|PER ?CENT|PERCENTAGE")
MARKER_PATTERN = re.compile(r"\((?:R|P|E)\.?\)|[#*†‡]")
WINDOW_PATTERN = re.compile(r"^([A-Z]{3})[A-Z]*-([A-Z]{3})[A-Z]*$")
QUARTER_PATTERN = re.compile(r"^Q([1-4])$")
TWO_DIGIT_YEAR_PATTERN = re.compile(r"^'?(\d{2})$")
SPACED_HYPHEN_PATTERN = re.compile(r"\s*-\s*")
DIGIT_THEN_LETTER_PATTERN = re.compile(r"(?<=\d)(?=[A-Z])")
WORD_THEN_DIGIT_PATTERN = re.compile(r"(?<=[A-Z]{3})(?=\d)")
WORKBOOK_SUFFIXES = (".xls", ".xlsx")
XLSX_MARKER = "[Content_Types].xml"
MONTH_NAMES = [
    ("JAN", "JANUARY"),
    ("FEB", "FEBRUARY"),
    ("MAR", "MARCH"),
    ("APR", "APRIL"),
    ("MAY",),
    ("JUN", "JUNE"),
    ("JUL", "JULY"),
    ("AUG", "AUGUST"),
    ("SEP", "SEPT", "SEPTEMBER"),
    ("OCT", "OCTOBER"),
    ("NOV", "NOVEMBER"),
    ("DEC", "DECEMBER"),
]
MONTHS = {
    name: number for number, names in enumerate(MONTH_NAMES, start=1) for name in names
}
EARLIEST_YEAR = 1900
LATEST_YEAR = 2100
TWENTIETH_CENTURY = 1900
TWENTY_FIRST_CENTURY = 2000
TWO_DIGIT_YEAR_PIVOT = 50
MONTHS_IN_YEAR = 12
MONTHS_IN_QUARTER = 3
MIN_CODES_IN_HEADER_ROW = 2
TITLE_SEARCH_ROWS = 3
ZERO_MARKERS = frozenset({"-", "–"})
SUPPRESSED_MARKERS = frozenset({"X"})
PERIOD_BASIS = "period"
THREE_MONTH_BASIS = "3m_ending"
MONTH_KIND = "month"
WINDOW_KIND = "window"
QUARTER_KIND = "quarter"
YEAR_KIND = "year"
FREQUENCY_CODES = {"Y": "A", "Q": "Q", "M": "M"}
SERIES_COLUMNS = [
    "series_id",
    "frequency",
    "reference_period",
    "reference_period_basis",
    "value",
]


@dataclass(frozen=True)
class RowLabel:
    """What the label cells of one table row say.

    Attributes:
        year: A four-digit year, if one is given.
        month: A month number, if one is given.
        quarter: A quarter number, if one is given.
        window_end: The last month of a three-month window ("Mar-May"), if given.
        date: A spreadsheet date, if the label is one.
        words: Any other text in the label.
    """

    year: int | None = None
    month: int | None = None
    quarter: int | None = None
    window_end: int | None = None
    date: pd.Timestamp | None = None
    words: str = ""

    @property
    def has_period(self) -> bool:
        """Whether the label gives any part of a period."""
        parts = (self.year, self.month, self.quarter, self.window_end, self.date)
        return any(part is not None for part in parts)


@dataclass(frozen=True)
class BlockRow:
    """One figure read from a column, with the period it describes.

    Attributes:
        period: The month, quarter or year.
        basis: "period", or "3m_ending" for a three-month window.
        value: The figure.
    """

    period: pd.Period
    basis: str
    value: float


def read_workbooks(
    content: bytes, name: str
) -> Iterator[tuple[str, dict[str, pd.DataFrame]]]:
    """Opens a downloaded spreadsheet, or every spreadsheet inside a zip file.

    A zip file is told from a spreadsheet by its contents, since some old
    releases saved zip files under a spreadsheet's name.

    Args:
        content: The downloaded bytes.
        name: The file name, used to name the workbooks.

    Yields:
        The name of each workbook and its sheets, read without headers.
    """
    if not _is_zip_archive(content):
        yield name, _read_sheets(content)
        return
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        members = sorted(
            member
            for member in archive.namelist()
            if member.lower().endswith(WORKBOOK_SUFFIXES)
        )
        for member in members:
            yield f"{name}:{member}", _read_sheets(archive.read(member))


def series_in_sheet(sheet: pd.DataFrame, bases: Mapping[str, str]) -> pd.DataFrame:
    """Reads every column in a sheet that is headed by one of the wanted series codes.

    Args:
        sheet: The sheet, read without headers.
        bases: Each wanted series code mapped to its basis ("period" or
            "3m_ending").

    Returns:
        One row per figure, with "series_id", "frequency", "reference_period"
        (as a string), "reference_period_basis" and "value" columns, in the
        order the columns and rows appear in the sheet.
    """
    cells = sheet.to_numpy(dtype=object)
    found = [
        (row, column, code)
        for row in range(cells.shape[0])
        for column in range(cells.shape[1])
        if (code := _code_in(cells[row, column])) in bases
    ]
    frames = [
        _block_frame(code, read_block(cells, row, column, bases[code]))
        for row, column, code in found
    ]
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=SERIES_COLUMNS)
    return pd.concat(frames, ignore_index=True)


def table_title(sheet: pd.DataFrame) -> str:
    """Gives a table's title: the first text near the top of its sheet.

    Args:
        sheet: The sheet, read without headers.

    Returns:
        The title, or an empty string if the top rows hold no text.
    """
    top = sheet.head(TITLE_SEARCH_ROWS).to_numpy(dtype=object).ravel()
    return next(
        (
            " ".join(cell.split())
            for cell in top
            if isinstance(cell, str) and cell.strip()
        ),
        "",
    )


def read_block(
    cells: object, header_row: int, column: int, basis: str
) -> list[BlockRow]:
    """Reads one column of figures downwards from the code that heads it.

    Rows whose labels hold words are headings or notes: before the figures
    begin they are passed over (a year in them is carried down), unless they
    announce changes or growth rates, in which case the column is not a level
    and nothing is read; after the figures begin they end the table, unless
    they repeat a heading from before the figures, which is how some tables
    follow annual figures with quarterly ones of the same measure.

    Args:
        cells: The sheet as a two-dimensional array.
        header_row: The row holding the series code.
        column: The column holding the series code.
        basis: The series' basis: "period" or "3m_ending".

    Returns:
        The figures with their periods, in the order they appear.
    """
    stub_width = _stub_width(cells[header_row])
    labelled: list[tuple[RowLabel, float]] = []
    headings: set[str] = set()
    heading_year: int | None = None
    for row in range(header_row + 1, cells.shape[0]):
        if _is_header_row(cells[row]):
            break
        label = read_label(cells[row, :stub_width])
        if label.words and labelled:
            if label.words in headings:
                continue
            break
        if label.words:
            if CHANGE_HEADING_PATTERN.search(label.words):
                return []
            headings.add(label.words)
            heading_year = label.year or heading_year
        elif label.has_period:
            labelled.append((label, _number(cells[row, column])))
    return resolve_periods(labelled, basis, heading_year)


def read_label(cells: object) -> RowLabel:
    """Works out what the label cells at the left of a row say.

    The first year given is the row's year: where a table's figures run into
    its label cells, a whole-number figure such as 2069 must not replace it.

    Args:
        cells: The row's label cells.

    Returns:
        The parts of a period the label gives, and any other words.
    """
    parts: dict[str, object] = {}
    words: list[str] = []
    for cell in cells:
        if isinstance(cell, datetime):
            parts["date"] = pd.Timestamp(cell)
        elif _is_year(cell):
            parts.setdefault("year", int(cell))
        elif isinstance(cell, str):
            _read_text_label(cell, parts, words)
    return RowLabel(**parts, words=" ".join(words))


def resolve_periods(
    labelled: list[tuple[RowLabel, float]], basis: str, carried_year: int | None
) -> list[BlockRow]:
    """Turns a column's row labels into periods, carrying years down the column.

    A month or quarter given without a year takes the year of the row above,
    moving on a year when the months go back to the start. Dates and months
    spaced three months apart are read as quarters, and twelve apart as years.

    Args:
        labelled: Each row's label and figure, in order.
        basis: The series' basis.
        carried_year: The year in force before the first row.

    Returns:
        The rows that have both a readable period and a figure.
    """
    candidates: list[tuple[str, pd.Period, float]] = []
    previous_position = 0
    for label, value in labelled:
        month = label.window_end or label.month
        position = month or (label.quarter or 0) * MONTHS_IN_QUARTER
        year = label.year or carried_year
        if label.year is None and year is not None and 0 < position < previous_position:
            year += 1
        carried_year, previous_position = year, position
        candidate = _candidate(label, year, month)
        if candidate is not None:
            candidates.append((*candidate, value))
    spacing = _usual_spacing(
        [period for kind, period, _ in candidates if kind == MONTH_KIND]
    )
    rows = [
        _block_row(kind, period, value, basis, spacing)
        for kind, period, value in candidates
        if not pd.isna(value)
    ]
    return [row for row in rows if row is not None]


def _is_zip_archive(content: bytes) -> bool:
    """Checks whether downloaded bytes are a zip file of workbooks.

    Args:
        content: The downloaded bytes.

    Returns:
        True for a zip file that is not itself an xlsx workbook.
    """
    if not zipfile.is_zipfile(io.BytesIO(content)):
        return False
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        return XLSX_MARKER not in archive.namelist()


def _read_sheets(content: bytes) -> dict[str, pd.DataFrame]:
    """Reads every sheet of a workbook without headers.

    Args:
        content: The workbook's bytes.

    Returns:
        Sheet name to sheet.
    """
    return pd.read_excel(io.BytesIO(content), sheet_name=None, header=None)


def _read_text_label(text: str, parts: dict[str, object], words: list[str]) -> None:
    """Reads the period parts out of a text label.

    Args:
        text: The label text.
        parts: Period parts found so far, updated in place.
        words: Other words found so far, extended in place.
    """
    for token in _tokens(text):
        window = WINDOW_PATTERN.match(token)
        quarter = QUARTER_PATTERN.match(token)
        two_digit_year = TWO_DIGIT_YEAR_PATTERN.match(token)
        if _is_year(token):
            parts.setdefault("year", int(token))
        elif token in MONTHS:
            parts["month"] = MONTHS[token]
        elif window and window[1] in MONTHS and window[2] in MONTHS:
            parts["window_end"] = MONTHS[window[2]]
        elif quarter:
            parts["quarter"] = int(quarter[1])
        elif two_digit_year and "month" in parts:
            parts.setdefault("year", _full_year(int(two_digit_year[1])))
        elif not _is_figure_or_symbol(token):
            words.append(token)


def _is_figure_or_symbol(token: str) -> bool:
    """Checks whether a token is a figure or an ONS symbol rather than a word.

    Where a table's labels run into its columns of figures, figures stored as
    text and symbols such as "-" (zero), ".." (not available) and "x"
    (suppressed) can turn up among the label cells; they are not labels.

    Args:
        token: A token from a label cell, in capitals.

    Returns:
        True if the token is a number, or has no letters or digits, or is "X".
    """
    if token in SUPPRESSED_MARKERS or not any(
        character.isalnum() for character in token
    ):
        return True
    return not pd.isna(pd.to_numeric(token, errors="coerce"))


def _tokens(text: str) -> list[str]:
    """Splits a label into tokens, tidying the ways ONS spaced its labels.

    "2006Oct" becomes "2006 OCT", "Nov- Jan" becomes "NOV-JAN", and revision
    and footnote markers such as "(r)", "(r.)" and "#" are dropped.

    Args:
        text: The label text.

    Returns:
        The tokens, in capitals.
    """
    tidied = MARKER_PATTERN.sub(" ", text.upper())
    tidied = SPACED_HYPHEN_PATTERN.sub("-", tidied)
    tidied = DIGIT_THEN_LETTER_PATTERN.sub(" ", tidied)
    tidied = WORD_THEN_DIGIT_PATTERN.sub(" ", tidied)
    return tidied.split()


def _full_year(two_digit_year: int) -> int:
    """Gives the full year a two-digit year stands for, as in "Jun 59" or "Mar 11".

    Args:
        two_digit_year: The last two digits of the year.

    Returns:
        A year from 1950 to 2049.
    """
    century = (
        TWENTIETH_CENTURY
        if two_digit_year >= TWO_DIGIT_YEAR_PIVOT
        else TWENTY_FIRST_CENTURY
    )
    return century + two_digit_year


def _candidate(
    label: RowLabel, year: int | None, month: int | None
) -> tuple[str, pd.Period] | None:
    """Gives the kind of period a row label describes, and the period.

    Args:
        label: The row's label.
        year: The row's year, after carrying down.
        month: The row's month, or the last month of its window.

    Returns:
        "month", "window", "quarter" or "year" with the period, or None if
        the label does not pin down a period.
    """
    if label.date is not None:
        return MONTH_KIND, label.date.to_period("M")
    if year is None:
        return None
    if label.window_end is not None:
        return WINDOW_KIND, pd.Period(year=year, month=month, freq="M")
    if month is not None:
        return MONTH_KIND, pd.Period(year=year, month=month, freq="M")
    if label.quarter is not None:
        return QUARTER_KIND, pd.Period(year=year, quarter=label.quarter, freq="Q-DEC")
    return YEAR_KIND, pd.Period(year=year, freq="Y-DEC")


def _block_row(
    kind: str, period: pd.Period, value: float, basis: str, spacing: int
) -> BlockRow | None:
    """Gives the period and basis of one row, if the series' basis allows it.

    A three-month window is kept only for series stored by the month their
    window ends, and a single month only for series that are not.

    Args:
        kind: "month", "window", "quarter" or "year".
        period: The period the label gives.
        value: The row's figure.
        basis: The series' basis.
        spacing: The usual gap in months between the column's monthly labels.

    Returns:
        The row, or None if its period does not suit the series.
    """
    if kind == WINDOW_KIND:
        return BlockRow(period, basis, value) if basis == THREE_MONTH_BASIS else None
    if kind != MONTH_KIND:
        return BlockRow(period, PERIOD_BASIS, value)
    if spacing == MONTHS_IN_YEAR:
        return BlockRow(period.asfreq("Y-DEC"), PERIOD_BASIS, value)
    if spacing == MONTHS_IN_QUARTER:
        return BlockRow(period.asfreq("Q-DEC"), PERIOD_BASIS, value)
    if basis == THREE_MONTH_BASIS:
        return None
    return BlockRow(period, PERIOD_BASIS, value)


def _usual_spacing(months: list[pd.Period]) -> int:
    """Finds the most common gap, in months, between consecutive monthly labels.

    Args:
        months: The monthly periods of a column, in order.

    Returns:
        The most common forward gap (the smallest, if several are equally
        common), or 1 if there are fewer than two labels.
    """
    gaps = [later.ordinal - earlier.ordinal for earlier, later in pairwise(months)]
    forward = [gap for gap in gaps if gap > 0]
    if not forward:
        return 1
    return int(pd.Series(forward).mode().min())


def _block_frame(code: str, rows: list[BlockRow]) -> pd.DataFrame:
    """Turns the rows read from one column into a table.

    Args:
        code: The series code.
        rows: The rows.

    Returns:
        One row per figure.
    """
    return pd.DataFrame(
        {
            "series_id": [code] * len(rows),
            "frequency": [FREQUENCY_CODES[row.period.freqstr[0]] for row in rows],
            "reference_period": [period_to_string(row.period) for row in rows],
            "reference_period_basis": [row.basis for row in rows],
            "value": [row.value for row in rows],
        },
        columns=SERIES_COLUMNS,
    )


def _stub_width(header: object) -> int:
    """Finds how many columns at the left of a table hold row labels.

    Args:
        header: The row of series codes.

    Returns:
        The column of the first series code.
    """
    return next(
        (column for column, cell in enumerate(header) if _code_in(cell)),
        len(header),
    )


def _is_header_row(cells: object) -> bool:
    """Checks whether a row is a row of series codes, which starts a new table.

    Args:
        cells: The row.

    Returns:
        True if at least two cells hold series codes.
    """
    return sum(bool(_code_in(cell)) for cell in cells) >= MIN_CODES_IN_HEADER_ROW


def _code_in(cell: object) -> str | None:
    """Gives the series code a cell holds, if it holds one.

    Args:
        cell: The cell.

    Returns:
        The code, or None.
    """
    if not isinstance(cell, str):
        return None
    text = cell.strip()
    return text if CODE_PATTERN.match(text) else None


def _is_year(cell: object) -> bool:
    """Checks whether a cell holds a plausible four-digit year.

    Args:
        cell: The cell: a number or a string.

    Returns:
        True for whole numbers from 1900 to 2100.
    """
    if isinstance(cell, str):
        return (
            len(cell) == 4
            and cell.isdigit()
            and EARLIEST_YEAR <= int(cell) <= LATEST_YEAR
        )
    if isinstance(cell, bool) or not isinstance(cell, Real) or pd.isna(cell):
        return False
    return float(cell).is_integer() and EARLIEST_YEAR <= cell <= LATEST_YEAR


def _number(cell: object) -> float:
    """Reads a figure from a cell.

    ONS marks a figure that is zero or rounds to zero with a dash ("-"), so a
    cell holding only a dash reads as 0. Other symbols, such as ".." for not
    available, read as missing.

    Args:
        cell: The cell.

    Returns:
        The number, or NaN if the cell does not hold one.
    """
    if isinstance(cell, bool):
        return float("nan")
    if isinstance(cell, Real):
        return float(cell)
    if isinstance(cell, str):
        text = cell.replace(",", "").strip()
        if text in ZERO_MARKERS:
            return 0.0
        return float(pd.to_numeric(text, errors="coerce"))
    return float("nan")
