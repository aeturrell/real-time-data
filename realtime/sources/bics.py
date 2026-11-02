"""ONS Business Insights and Conditions Survey results, read from every wave's file.

Each fortnightly wave has its own edition on the ONS website, whose time-series
worksheets repeat the results of earlier waves. Every edition is therefore a true
vintage, dated by its release, and a corrected edition is another vintage dated by
its correction. Time-series worksheets begin with wave 30 (May 2021), so earlier
waves first appear then at the soonest. Some editions give a release date before their survey
closed, which cannot be right; see dependable_release.

The dates beside each result describe the period the question asked about, which
differs between questions. Each wave is instead stored against the last day of
its survey window, read from the newest file's "Dates" worksheet, so that every
question from the same wave shares one reference date.
"""

import re
from bisect import bisect_right
from collections.abc import Iterator
from dataclasses import dataclass

import pandas as pd
from loguru import logger
from openpyxl.workbook.workbook import Workbook

from realtime.cache import CachedFile, DownloadCache, check_is_workbook
from realtime.config import SurveySpec
from realtime.settings import LONDON_TIMEZONE, ONS_BASE_URL
from realtime.sources.base import ONS_SOURCE
from realtime.sources.survey_tables import (
    READ_ERRORS,
    Row,
    archive_rows,
    clean_text,
    find_column,
    find_sheet,
    open_workbook,
    read_rows,
    to_number,
)

ROUTE_NAME = "ons_bics"
SURVEY_NAME = "BICS"
FREQUENCY = "D"
PRECISION = "day"
UPPER_BOUND = "upper_bound"
DAYS_AFTER_CLOSE_IF_NOTHING_FOLLOWS = 14
DATASET_PATH = (
    "/economy/economicoutputandproductivity/output/datasets/"
    "businessinsightsandimpactontheukeconomy"
)
DATES_SHEET_PATTERN = r"^dates$"
SPREADSHEET_EXTENSIONS = (".xlsx", ".xls")
ALL_BUSINESSES = "All businesses"
WAVE_HEADING = "Wave"
INDUSTRY_HEADING = "Industry/Size Band"
TRADING_STATUS_HEADING = "Trading Status"
WAVE_PATTERN = re.compile(r"wave\s*0*(\d+)", re.IGNORECASE)
WINDOW_END_PATTERN = re.compile(r"\bto\s+(\d{1,2}\s+[A-Za-z]+\s+\d{4})")
DAYS_CORRECTIONS_ARE_EXPECTED = 92
SUBTITLE_ROW = 1


@dataclass(frozen=True)
class EditionFile:
    """One published version of a wave's spreadsheet.

    Attributes:
        url: The spreadsheet's address.
        published: The London date this version appeared.
        precision: How exactly that date is known.
        is_settled: Whether this version's address can no longer change.
    """

    url: str
    published: pd.Timestamp
    precision: str
    is_settled: bool


@dataclass(frozen=True)
class Edition:
    """One wave's page on the ONS website.

    Attributes:
        wave: The wave number.
        uri: The edition's path.
        document: Its JSON description.
        released: The release date ONS gives, as a London date.
        is_settled: Whether corrections are no longer expected.
    """

    wave: int
    uri: str
    document: dict
    released: pd.Timestamp
    is_settled: bool


class BicsSource:
    """Collects the BICS results listed in the configuration.

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
        """Yields the results held in each published version of each wave's file.

        Yields:
            One table per version, oldest first, so that a same-day correction
            comes after the version it corrects.
        """
        if not self._specs:
            return
        try:
            editions = list(self._editions())
            newest = max(editions, key=lambda edition: edition.wave)
            window_ends = self._newest_window_ends(newest)
            files = sorted(
                self._all_files(editions, window_ends),
                key=lambda file: file.published,
            )
        except READ_ERRORS as error:
            logger.error(f"Skipping BICS: {error}")
            return
        for file in files:
            try:
                frame = self._read_file(file, window_ends)
            except READ_ERRORS as error:
                logger.error(f"Skipping BICS file {file.url}: {error}")
                continue
            if frame is not None:
                yield frame

    def _editions(self) -> Iterator[Edition]:
        """Reads the description of every wave's edition.

        An edition can be corrected for a while after its release, so recent
        editions are checked again on every run. A correction to an older edition
        is not noticed unless its cached copy is removed.

        Yields:
            Each edition that can be read.
        """
        landing = self._cache.fetch_json(
            f"{ONS_BASE_URL}{DATASET_PATH}/data", immutable=False
        )
        for item in landing["datasets"]:
            uri = item["uri"]
            url = f"{ONS_BASE_URL}{uri}/data"
            try:
                document = self._cache.fetch_json(url, immutable=True)
                released = london_date(document["description"]["releaseDate"])
                age = pd.Timestamp.now().normalize() - released
                is_settled = age >= pd.Timedelta(days=DAYS_CORRECTIONS_ARE_EXPECTED)
                if not is_settled:
                    document = self._cache.fetch_json(url, immutable=False)
                wave = WAVE_PATTERN.search(uri.rsplit("/", 1)[-1])
                if wave is None:
                    raise ValueError(f"no wave number in {uri}")
                yield Edition(int(wave[1]), uri, document, released, is_settled)
            except READ_ERRORS as error:
                logger.error(f"Skipping BICS edition {uri}: {error}")

    def _newest_window_ends(self, newest: Edition) -> dict[int, pd.Timestamp]:
        """Reads the survey windows of every wave from the newest file.

        Older files hold a few mistyped dates, such as the wrong year for wave 55,
        which later files correct.

        Args:
            newest: The newest edition.

        Returns:
            Wave numbers mapped to the last day of their survey window.

        Raises:
            ValueError: If the newest file has no "Dates" worksheet.
        """
        file = _spreadsheet_file(
            newest.uri, newest.document, newest.released, PRECISION, newest.is_settled
        )
        workbook = open_workbook(self._download(file))
        dates_sheet = find_sheet(workbook, DATES_SHEET_PATTERN)
        if dates_sheet is None:
            raise ValueError(f"no Dates worksheet in {file.url}")
        return read_window_ends(read_rows(workbook, dates_sheet))

    def _all_files(
        self, editions: list[Edition], window_ends: dict[int, pd.Timestamp]
    ) -> Iterator[EditionFile]:
        """Lists every published version of every wave's spreadsheet.

        Args:
            editions: Every edition.
            window_ends: Wave numbers mapped to the last day of their survey window.

        Yields:
            Each version.
        """
        release_dates = sorted({edition.released for edition in editions})
        for edition in editions:
            try:
                yield from self._edition_files(edition, window_ends, release_dates)
            except READ_ERRORS as error:
                logger.error(f"Skipping BICS edition {edition.uri}: {error}")

    def _edition_files(
        self,
        edition: Edition,
        window_ends: dict[int, pd.Timestamp],
        release_dates: list[pd.Timestamp],
    ) -> Iterator[EditionFile]:
        """Lists the published versions of one wave's spreadsheet.

        Args:
            edition: The edition.
            window_ends: Wave numbers mapped to the last day of their survey window.
            release_dates: The release dates of every edition, sorted.

        Yields:
            The superseded versions, then the current one.
        """
        versions = edition.document.get("versions") or []
        window_end = window_ends.get(edition.wave)
        released, precision = dependable_release(
            edition.released, window_end, release_dates
        )
        dates = version_dates(released, versions)
        for position, (version, published) in enumerate(zip(versions, dates)):
            older = self._cache.fetch_json(
                f"{ONS_BASE_URL}{version['uri']}/data", immutable=True
            )
            yield _spreadsheet_file(
                version["uri"],
                older,
                published,
                precision if position == 0 else PRECISION,
                True,
            )
        current_precision = PRECISION if versions else precision
        yield _spreadsheet_file(
            edition.uri,
            edition.document,
            dates[-1],
            current_precision,
            edition.is_settled,
        )

    def _download(self, file: EditionFile) -> CachedFile:
        """Downloads a version's spreadsheet if needed.

        Args:
            file: The version.

        Returns:
            The cached file.
        """
        return self._cache.fetch(
            file.url, immutable=file.is_settled, validate=check_is_workbook
        )

    def _read_file(
        self, file: EditionFile, window_ends: dict[int, pd.Timestamp]
    ) -> pd.DataFrame | None:
        """Reads every configured series from one version of a wave's file.

        Args:
            file: The version.
            window_ends: Wave numbers mapped to the last day of their survey window.

        Returns:
            The series' rows, or None if the file has no "Dates" worksheet and
            so comes from before the time-series worksheets began.
        """
        cached = self._download(file)
        workbook = open_workbook(cached)
        if find_sheet(workbook, DATES_SHEET_PATTERN) is None:
            return None
        frames = [
            frame
            for spec in self._specs
            if (frame := _series_rows(spec, workbook, window_ends, file, cached))
            is not None
        ]
        return pd.concat(frames, ignore_index=True) if frames else None


def london_date(timestamp: str) -> pd.Timestamp:
    """Reads an ONS timestamp, stored in UTC, as a date in London.

    Args:
        timestamp: For example "2026-10-07T23:00:00.000Z", which is 8 October.

    Returns:
        The London date.
    """
    moment = pd.Timestamp(timestamp).tz_convert(LONDON_TIMEZONE)
    return moment.tz_localize(None).normalize()


def dependable_release(
    released: pd.Timestamp,
    window_end: pd.Timestamp | None,
    release_dates: list[pd.Timestamp],
) -> tuple[pd.Timestamp, str]:
    """Replaces a release date that falls before its survey had even closed.

    About one edition in eight still carries the previous wave's release date. Such
    a date cannot be right, so it gives way to the next release after the survey
    closed, the latest the wave can have come out if ONS published in order.

    Args:
        released: The release date ONS gives.
        window_end: The last day of the wave's survey window, if known.
        release_dates: The release dates of every edition, sorted.

    Returns:
        The date to use and its precision.
    """
    if window_end is None or released > window_end:
        return released, PRECISION
    position = bisect_right(release_dates, window_end)
    if position == len(release_dates):
        return window_end + pd.Timedelta(
            days=DAYS_AFTER_CLOSE_IF_NOTHING_FOLLOWS
        ), UPPER_BOUND
    return release_dates[position], UPPER_BOUND


def version_dates(
    released: pd.Timestamp, versions: list[dict[str, str]]
) -> list[pd.Timestamp]:
    """Gives the date each version of a wave's file first appeared.

    ONS records, for each superseded version, the moment it was replaced. The first
    version appeared at release, and each later one when the one before it was
    replaced.

    Args:
        released: The edition's release date.
        versions: The superseded versions, oldest first.

    Returns:
        One date per superseded version, then one for the current version.
    """
    replaced = [london_date(version["updateDate"]) for version in versions]
    first_appeared = min([released, *replaced[:1]])
    return [first_appeared, *replaced]


def read_window_ends(rows: list[Row]) -> dict[int, pd.Timestamp]:
    """Reads the last day of each wave's survey window from the "Dates" worksheet.

    Args:
        rows: The worksheet's rows: wave, reference period and survey window.

    Returns:
        Wave numbers mapped to the last day of their survey window.

    Raises:
        ValueError: If the worksheet has no recognisable wave.
    """
    header = _heading_row(rows, (WAVE_HEADING, "Survey live period"))
    window_column = [clean_text(cell) for cell in rows[header]].index(
        "Survey live period"
    )
    ends: dict[int, pd.Timestamp] = {}
    for row in rows[header + 1 :]:
        wave = WAVE_PATTERN.search(clean_text(row[0]))
        end = WINDOW_END_PATTERN.search(clean_text(row[window_column]))
        if wave and end:
            ends[int(wave[1])] = pd.Timestamp(end[1])
    if not ends:
        raise ValueError("no survey windows found")
    return ends


def read_answer_table(
    rows: list[Row], answer: str, trading_status: str | None
) -> dict[int, float]:
    """Reads the all-businesses results for one answer to one question.

    Args:
        rows: The worksheet's rows.
        answer: The answer's column heading.
        trading_status: The group of businesses the results must describe.

    Returns:
        Wave numbers mapped to values, empty if the worksheet has no such answer
        or describes a different group of businesses.
    """
    header = _heading_row(rows, (WAVE_HEADING, INDUSTRY_HEADING))
    headings = {
        position: heading
        for position, cell in enumerate(rows[header])
        if (heading := clean_text(cell))
    }
    column = find_column(headings, answer, ())
    if column is None:
        return {}
    positions = {heading: position for position, heading in headings.items()}
    status_column = positions.get(TRADING_STATUS_HEADING)
    if not _describes_group(rows, status_column, trading_status):
        return {}
    results: dict[int, float] = {}
    for row in rows[header + 1 :]:
        if clean_text(row[positions[INDUSTRY_HEADING]]) != ALL_BUSINESSES:
            continue
        if (
            status_column is not None
            and trading_status
            and clean_text(row[status_column]).lower() != trading_status.lower()
        ):
            continue
        wave = WAVE_PATTERN.search(clean_text(row[positions[WAVE_HEADING]]))
        value = to_number(row[column])
        if wave and value is not None:
            results[int(wave[1])] = value
    return results


def _describes_group(
    rows: list[Row], status_column: int | None, trading_status: str | None
) -> bool:
    """Checks that a worksheet covers the group of businesses a series describes.

    A worksheet either has a trading-status column or states its group in the line
    under its title.

    Args:
        rows: The worksheet's rows.
        status_column: The trading-status column, if there is one.
        trading_status: The group of businesses the series describes.

    Returns:
        True if the worksheet can be used.
    """
    if not trading_status or status_column is not None:
        return True
    return trading_status.lower() in clean_text(rows[SUBTITLE_ROW][0]).lower()


def _heading_row(rows: list[Row], headings: tuple[str, ...]) -> int:
    """Finds the row holding given column headings.

    Args:
        rows: The worksheet's rows.
        headings: Headings that must all be present.

    Returns:
        The row's position.

    Raises:
        ValueError: If no row has them all.
    """
    for index, row in enumerate(rows):
        cells = {clean_text(cell) for cell in row}
        if all(heading in cells for heading in headings):
            return index
    raise ValueError(f"no row with headings {headings}")


def _spreadsheet_file(
    uri: str,
    document: dict,
    published: pd.Timestamp,
    precision: str,
    is_settled: bool,
) -> EditionFile:
    """Finds the spreadsheet belonging to an edition or an earlier version of one.

    Args:
        uri: The path of the edition or version.
        document: Its JSON description.
        published: The date it appeared.
        precision: How exactly that date is known.
        is_settled: Whether it can no longer change.

    Returns:
        The file.

    Raises:
        ValueError: If it has no spreadsheet.
    """
    names = [item["file"] for item in document.get("downloads", [])]
    spreadsheets = [
        name for name in names if name.lower().endswith(SPREADSHEET_EXTENSIONS)
    ]
    if not spreadsheets:
        raise ValueError(f"no spreadsheet for {uri}")
    url = f"{ONS_BASE_URL}/file?uri={uri}/{spreadsheets[0]}"
    return EditionFile(
        url=url, published=published, precision=precision, is_settled=is_settled
    )


def _series_rows(
    spec: SurveySpec,
    workbook: Workbook,
    window_ends: dict[int, pd.Timestamp],
    file: EditionFile,
    cached: CachedFile,
) -> pd.DataFrame | None:
    """Reads one series from a version of a wave's file.

    Args:
        spec: The series.
        workbook: The open workbook.
        window_ends: Wave numbers mapped to the last day of their survey window.
        file: The version.
        cached: The downloaded file.

    Returns:
        The series' rows, or None if the file does not hold the series.
    """
    sheet = find_sheet(workbook, spec.sheet)
    if sheet is None or spec.answer is None:
        return None
    results = read_answer_table(
        read_rows(workbook, sheet), spec.answer, spec.trading_status
    )
    records = [
        {
            "reference_period": str(window_ends[wave].date()),
            "vintage_date": file.published,
            "value": value,
            "vintage_date_precision": file.precision,
        }
        for wave, value in results.items()
        if wave in window_ends
    ]
    if not records:
        return None
    return archive_rows(
        spec, pd.DataFrame(records), FREQUENCY, ROUTE_NAME, ONS_SOURCE, cached
    )
