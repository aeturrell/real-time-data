"""Bank of England Decision Maker Panel results, read from each month's data file.

Each month's results page links to a spreadsheet holding the survey's whole history
as it stood on that day, so every file is a true vintage. The sheets holding the
headline figures appear from late 2021 or 2022, so earlier survey months first
appear then.
"""

from collections.abc import Iterator
from datetime import datetime

import pandas as pd
from loguru import logger

from realtime.cache import DownloadCache
from realtime.config import SurveySpec
from realtime.periods import parse_period_label, period_to_string
from realtime.sources.base import BANK_OF_ENGLAND_SOURCE
from realtime.sources.boe_site import BankPage, download_spreadsheet, released_pages
from realtime.sources.survey_tables import (
    READ_ERRORS,
    SurveyTable,
    archive_rows,
    find_column,
    find_sheet,
    open_workbook,
    read_rows,
    read_survey_table,
    to_number,
)

ROUTE_NAME = "boe_dmp"
SURVEY_NAME = "DMP"
FREQUENCY = "M"
PRECISION = "day"
SECTION = "decision-maker-panel"
FILE_MARKERS = ("monthly", "quarterly")


class DecisionMakerPanelSource:
    """Collects the Decision Maker Panel results listed in the configuration.

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
        """Yields the results held in each month's file.

        Yields:
            One table per file, covering every configured series it holds.
        """
        if not self._specs:
            return
        try:
            pages = list(released_pages(self._cache, SECTION))
        except READ_ERRORS as error:
            logger.error(f"Skipping Decision Maker Panel: {error}")
            return
        for page in pages:
            url = data_file_link(page)
            if url is None:
                logger.warning(f"No data file for {page.path}")
                continue
            try:
                yield self._read_file(page, url)
            except READ_ERRORS as error:
                logger.error(f"Skipping data file {url}: {error}")

    def _read_file(self, page: BankPage, url: str) -> pd.DataFrame:
        """Reads every configured series from one file.

        Args:
            page: The month's page.
            url: The file's address.

        Returns:
            The series' rows, dated by the page.
        """
        cached = download_spreadsheet(self._cache, url)
        workbook = open_workbook(cached)
        frames = []
        for sheet_pattern in dict.fromkeys(spec.sheet for spec in self._specs):
            sheet = find_sheet(workbook, sheet_pattern)
            if sheet is None:
                continue
            table = read_survey_table(read_rows(workbook, sheet), is_survey_month)
            for spec in self._specs:
                if spec.sheet != sheet_pattern:
                    continue
                try:
                    observations = _observations(spec, table, page)
                except ValueError as error:
                    logger.error(f"Skipping {spec.series_id} in {url}: {error}")
                    continue
                if not observations.empty:
                    frames.append(
                        archive_rows(
                            spec,
                            observations,
                            FREQUENCY,
                            ROUTE_NAME,
                            BANK_OF_ENGLAND_SOURCE,
                            cached,
                        )
                    )
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def data_file_link(page: BankPage) -> str | None:
    """Picks a month's own data file from the spreadsheets its page links to.

    Most months have a "monthly" file. A few have only a "quarterly" one, and some
    pages also link to an earlier month's file, which is ignored.

    Args:
        page: The month's page.

    Returns:
        The file's address, or None if the page has no file of its own.
    """
    own_files = [url for url in page.spreadsheets if page.slug in url.lower()]
    for marker in FILE_MARKERS:
        for url in own_files:
            if marker in url.rsplit("/", 1)[-1].lower():
                return url
    return None


def is_survey_month(label: object) -> bool:
    """Checks whether a first-column cell labels a survey month.

    Args:
        label: The cell's contents, which may be a date or text such as "Jan-17".

    Returns:
        True for a recognisable month.
    """
    if not isinstance(label, datetime | str):
        return False
    return parse_period_label(label, FREQUENCY) is not None


def _observations(spec: SurveySpec, table: SurveyTable, page: BankPage) -> pd.DataFrame:
    """Reads one series from a file's table.

    Args:
        spec: The series.
        table: The worksheet's table.
        page: The month's page.

    Returns:
        The series' rows, empty if this file does not have its column yet.
    """
    column = find_column(table.columns, spec.answer, spec.terms)
    if column is None and spec.older_answer:
        column = find_column(table.columns, spec.older_answer, ())
    if column is None:
        return pd.DataFrame()
    records = [
        {
            "reference_period": period_to_string(parse_period_label(row[0], FREQUENCY)),
            "vintage_date": page.published,
            "value": value,
            "vintage_date_precision": PRECISION,
        }
        for row in table.rows
        if column < len(row) and (value := to_number(row[column])) is not None
    ]
    return pd.DataFrame(records)
