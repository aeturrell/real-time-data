"""Bank of England Agents' scores, dated from the pattern of the Agents' reports.

The Bank publishes one file holding the whole history of the scores, so what was
published on any earlier date cannot be seen. The scores are treated as never
revised, and each month's row is dated from the Bank's own release pattern: the
first Agents' report published after the fifteenth of that month. That is often
later than the true date, and is never meant to be earlier, so the precision is
"upper_bound". A row newer than every report is dated the day the file was
downloaded.

Scores for months before a column became public are dated at the first report
from its "public_from" month, because nobody outside the Bank could see them.
"""

from bisect import bisect_left
from collections.abc import Iterator

import pandas as pd
from loguru import logger

from realtime.cache import CachedFile, DownloadCache, check_is_workbook
from realtime.config import SurveySpec
from realtime.periods import parse_period_label, period_to_string
from realtime.settings import BANK_OF_ENGLAND_BASE_URL
from realtime.sources.base import BANK_OF_ENGLAND_SOURCE
from realtime.sources.boe_site import released_pages
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

ROUTE_NAME = "boe_agents"
SURVEY_NAME = "AGENTS"
FREQUENCY = "M"
PRECISION = "upper_bound"
SCORES_FILE_URL = (
    f"{BANK_OF_ENGLAND_BASE_URL}/-/media/boe/files/agents-summary/agentsscores.xlsx"
)
REPORTS_SECTION = "agents-summary"
SCORES_SHEET_PATTERN = r"^agents' scores$"
LAST_DAY_BEFORE_ROW_COUNTS_AS_REPORTED = 15


class AgentsSource:
    """Collects the Agents' scores listed in the configuration.

    Attributes:
        name: The route code written to the "route" column.
    """

    name = ROUTE_NAME

    def __init__(self, specs: tuple[SurveySpec, ...], cache: DownloadCache) -> None:
        """Sets up the source.

        Args:
            specs: All configured survey series; only the Agents' scores are used.
            cache: The download cache.
        """
        self._specs = tuple(spec for spec in specs if spec.survey == SURVEY_NAME)
        self._cache = cache

    def fetch(self) -> Iterator[pd.DataFrame]:
        """Yields the dated scores of each configured series.

        Yields:
            One table per series.
        """
        if not self._specs:
            return
        try:
            cached = self._cache.fetch(
                SCORES_FILE_URL, immutable=False, validate=check_is_workbook
            )
            table = _read_scores(cached)
            report_dates = sorted(
                page.published for page in released_pages(self._cache, REPORTS_SECTION)
            )
        except READ_ERRORS as error:
            logger.error(f"Skipping Agents' scores: {error}")
            return
        for spec in self._specs:
            try:
                yield _series_rows(spec, table, report_dates, cached)
            except READ_ERRORS as error:
                logger.error(f"Skipping {spec.series_id}: {error}")


def report_date_for_month(
    month: pd.Period, report_dates: list[pd.Timestamp], retrieved: pd.Timestamp
) -> pd.Timestamp:
    """Gives the date by which a month's scores should have been published.

    Args:
        month: The month the scores are for.
        report_dates: The dates of every released report, sorted.
        retrieved: The day the file was downloaded, used if no report follows.

    Returns:
        The date of the first report after the fifteenth of the month.
    """
    cutoff = month.start_time + pd.Timedelta(
        days=LAST_DAY_BEFORE_ROW_COUNTS_AS_REPORTED
    )
    position = bisect_left(report_dates, cutoff)
    return report_dates[position] if position < len(report_dates) else retrieved


def first_report_from(
    month: pd.Period, report_dates: list[pd.Timestamp], retrieved: pd.Timestamp
) -> pd.Timestamp:
    """Gives the date of the first report in or after a month.

    Args:
        month: The month a column became public.
        report_dates: The dates of every released report, sorted.
        retrieved: The day the file was downloaded, used if no report follows.

    Returns:
        The report's date.
    """
    position = bisect_left(report_dates, month.start_time)
    return report_dates[position] if position < len(report_dates) else retrieved


def _read_scores(cached: CachedFile) -> SurveyTable:
    """Reads the worksheet of monthly scores.

    Args:
        cached: The downloaded file.

    Returns:
        The table.

    Raises:
        ValueError: If the worksheet is missing.
    """
    workbook = open_workbook(cached)
    sheet = find_sheet(workbook, SCORES_SHEET_PATTERN)
    if sheet is None:
        raise ValueError(f"no Agents' scores worksheet in {cached.url}")
    return read_survey_table(read_rows(workbook, sheet), _is_month_label)


def _is_month_label(label: object) -> bool:
    """Checks whether a first-column cell labels a month.

    Args:
        label: The cell's contents.

    Returns:
        True for a recognisable month.
    """
    return parse_period_label(label, FREQUENCY) is not None


def _series_rows(
    spec: SurveySpec,
    table: SurveyTable,
    report_dates: list[pd.Timestamp],
    cached: CachedFile,
) -> pd.DataFrame:
    """Builds the dated rows of one series.

    Args:
        spec: The series.
        table: The scores.
        report_dates: The dates of every released report, sorted.
        cached: The downloaded file.

    Returns:
        One row per month with a score.

    Raises:
        ValueError: If the column has gone, which means the file has changed.
    """
    column = find_column(table.columns, spec.answer, spec.terms)
    if column is None:
        raise ValueError(f"no column for {spec.answer} {spec.terms}")
    retrieved = cached.retrieved_at.tz_localize(None).normalize()
    public_from = first_report_from(
        pd.Period(spec.public_from, freq=FREQUENCY), report_dates, retrieved
    )
    records = [
        {
            "reference_period": period_to_string(month),
            "vintage_date": max(
                report_date_for_month(month, report_dates, retrieved), public_from
            ),
            "value": value,
            "vintage_date_precision": PRECISION,
        }
        for row in table.rows
        if (month := parse_period_label(row[0], FREQUENCY)) is not None
        and column < len(row)
        and (value := to_number(row[column])) is not None
    ]
    return archive_rows(
        spec,
        pd.DataFrame(records),
        FREQUENCY,
        ROUTE_NAME,
        BANK_OF_ENGLAND_SOURCE,
        cached,
    )
