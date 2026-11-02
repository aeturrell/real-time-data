"""Bank of England Credit Conditions Survey results, read from each quarter's annex.

Each quarter's results page links to a spreadsheet annex holding the whole history
of the survey as it stood on that day, so every annex is a true vintage. The annex
files begin in late 2015, and the first one to follow the usual layout gives the
earlier quarters, which the archive therefore labels "history".
"""

import re
from collections.abc import Iterator
from itertools import pairwise

import pandas as pd
from loguru import logger

from realtime.cache import DownloadCache
from realtime.config import SurveySpec
from realtime.periods import period_to_string
from realtime.sources.base import BANK_OF_ENGLAND_SOURCE
from realtime.sources.boe_site import BankPage, download_spreadsheet, released_pages
from realtime.sources.survey_tables import (
    READ_ERRORS,
    Row,
    archive_rows,
    clean_text,
    find_sheet,
    open_workbook,
    read_rows,
    to_number,
)

ROUTE_NAME = "boe_ccs"
SURVEY_NAME = "CCS"
FREQUENCY = "Q"
PRECISION = "day"
SECTION = "credit-conditions-survey"
SLUG_PATTERN = re.compile(r"^(\d{4})-(q[1-4])$")
FOOTNOTE_PATTERN = re.compile(r"\s*(\([a-z]\)|\*)+")
QUARTER_LABEL_PATTERN = re.compile(r"^q([1-4])\b", re.IGNORECASE)
EARLIEST_YEAR = 2000
LATEST_YEAR = 2100
QUESTION_COLUMN = 0
HORIZON_COLUMN = 1


class CreditConditionsSource:
    """Collects the Credit Conditions Survey results listed in the configuration.

    Attributes:
        name: The route code written to the "route" column.
    """

    name = ROUTE_NAME

    def __init__(self, specs: tuple[SurveySpec, ...], cache: DownloadCache) -> None:
        """Sets up the source.

        Args:
            specs: All configured survey series; only this survey's are used.
            cache: The download cache.
        """
        self._specs = tuple(spec for spec in specs if spec.survey == SURVEY_NAME)
        self._cache = cache

    def fetch(self) -> Iterator[pd.DataFrame]:
        """Yields the results held in each quarter's annex.

        Yields:
            One table per annex, covering every configured series it holds.
        """
        if not self._specs:
            return
        try:
            pages = list(released_pages(self._cache, SECTION))
        except READ_ERRORS as error:
            logger.error(f"Skipping Credit Conditions Survey: {error}")
            return
        for page in pages:
            url = annex_link(page)
            if url is None:
                logger.debug(f"No annex for {page.path}")
                continue
            try:
                yield self._read_annex(page, url)
            except READ_ERRORS as error:
                logger.error(f"Skipping annex {url}: {error}")

    def _read_annex(self, page: BankPage, url: str) -> pd.DataFrame:
        """Reads every configured series from one annex.

        Args:
            page: The quarter's page.
            url: The annex's address.

        Returns:
            The series' rows, dated by the page.
        """
        cached = download_spreadsheet(self._cache, url)
        workbook = open_workbook(cached)
        frames = []
        for sheet_pattern in dict.fromkeys(spec.sheet for spec in self._specs):
            sheet = find_sheet(workbook, sheet_pattern)
            if sheet is None:
                logger.warning(f"No sheet like {sheet_pattern} in {url}")
                continue
            values = read_annex_sheet(read_rows(workbook, sheet))
            frames += [
                archive_rows(
                    spec,
                    _observations(values[key], page),
                    FREQUENCY,
                    ROUTE_NAME,
                    BANK_OF_ENGLAND_SOURCE,
                    cached,
                )
                for spec in self._specs
                if spec.sheet == sheet_pattern
                and (key := (spec.question, spec.horizon)) in values
            ]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def annex_link(page: BankPage) -> str | None:
    """Picks a quarter's own annex from the spreadsheets its page links to.

    Some pages also link to a later quarter's annex, which is ignored.

    Args:
        page: The quarter's page.

    Returns:
        The annex's address, or None if the page has no annex of its own.
    """
    slug = SLUG_PATTERN.match(page.slug)
    if not slug:
        return None
    year, quarter = slug.groups()
    matches = (
        url
        for url in page.spreadsheets
        if year in (name := url.rsplit("/", 1)[-1].lower()) and quarter in name
    )
    return next(matches, None)


def read_annex_sheet(rows: list[Row]) -> dict[tuple[str, str], dict[pd.Period, float]]:
    """Reads one annex worksheet.

    Args:
        rows: The worksheet's rows.

    Returns:
        Question and horizon mapped to each quarter's value. A question is given
        once and applies to the rows below it until the next one.

    Raises:
        ValueError: If the worksheet has no row of years with quarters below.
    """
    header = _find_year_row(rows)
    quarters = _quarter_columns(rows[header], rows[header + 1])
    table: dict[tuple[str, str], dict[pd.Period, float]] = {}
    question = ""
    for row in rows[header + 2 :]:
        question = normalise_question(row[QUESTION_COLUMN]) or question
        horizon = clean_text(row[HORIZON_COLUMN]) if len(row) > HORIZON_COLUMN else ""
        values = {
            quarter: value
            for position, quarter in quarters.items()
            if position < len(row) and (value := to_number(row[position])) is not None
        }
        if values:
            table[(question, horizon)] = values
    return table


def normalise_question(label: object) -> str:
    """Tidies a question by removing footnote marks such as "(c)" and "*".

    Args:
        label: The cell's contents.

    Returns:
        The question.
    """
    return clean_text(FOOTNOTE_PATTERN.sub("", clean_text(label)))


def _find_year_row(rows: list[Row]) -> int:
    """Finds the row of years, which has a row of quarter labels directly below.

    Args:
        rows: The worksheet's rows.

    Returns:
        The row's position.

    Raises:
        ValueError: If there is no such row.
    """
    for index, (row, below) in enumerate(pairwise(rows)):
        has_year = any(_as_year(cell) is not None for cell in row)
        has_quarter = any(
            QUARTER_LABEL_PATTERN.match(clean_text(cell)) for cell in below
        )
        if has_year and has_quarter:
            return index
    raise ValueError("no row of years with quarters below it")


def _quarter_columns(year_row: Row, quarter_row: Row) -> dict[int, pd.Period]:
    """Reads which quarter each column holds.

    The first year written gives the starting point, and the year then moves on each
    time the quarter labels start again. Later years written in the row are not used,
    because some annexes misplace or mistype them: the 2019 Q2 annex puts 2016 two
    columns late, and the first 2016 annex's corporate sheet gives 2013 for 2014.

    Args:
        year_row: The row of years, each written once above its quarters.
        quarter_row: The row of quarter labels.

    Returns:
        Column positions mapped to quarters.
    """
    columns: dict[int, pd.Period] = {}
    year = None
    previous_quarter = None
    for position, cell in enumerate(quarter_row):
        if year is None and position < len(year_row):
            year = _as_year(year_row[position])
        label = QUARTER_LABEL_PATTERN.match(clean_text(cell))
        if not label or year is None:
            continue
        quarter = int(label[1])
        if previous_quarter is not None and quarter <= previous_quarter:
            year += 1
        columns[position] = pd.Period(year=year, quarter=quarter, freq="Q-DEC")
        previous_quarter = quarter
    return columns


def _as_year(cell: object) -> int | None:
    """Reads a cell as a plausible year.

    Args:
        cell: The cell's contents.

    Returns:
        The year, or None.
    """
    number = to_number(cell)
    if number is None or not number.is_integer():
        return None
    return int(number) if EARLIEST_YEAR < number < LATEST_YEAR else None


def _observations(values: dict[pd.Period, float], page: BankPage) -> pd.DataFrame:
    """Turns a series' values in one annex into rows dated by the page.

    Args:
        values: Quarters mapped to values.
        page: The quarter's page.

    Returns:
        The rows.
    """
    return pd.DataFrame(
        {
            "reference_period": [period_to_string(quarter) for quarter in values],
            "value": list(values.values()),
        }
    ).assign(vintage_date=page.published, vintage_date_precision=PRECISION)
