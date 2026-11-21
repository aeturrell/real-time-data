"""Vintages from releases on the old ONS website, as saved by the National Archives.

From 2011 until February 2016 every ONS release had its own folder on the old
website, for example /ons/rel/lms/labour-market-statistics/june-2013/, holding
the bulletin and its reference tables. The UK Government Web Archive kept
copies of these folders, and its index can be searched by machine.

For each configured release, every edition's folder is listed from the index,
the configured reference tables are downloaded, and the configured series are
read from them (see realtime.sources.release_tables). Each edition is dated by
the "DCTERMS.Issued" date on its release page (or its bulletin page), with
precision "day". If no saved copy of either gives one, the edition is dated by
the day its tables were first saved, an upper bound; and if that is not before
every later edition's date, the edition is left out, so that older figures
never appear to be newer ones.

Each series is read only from the tables configured for it, scaled into the
units of its ONS pages, because the same code can head figures in different
units in different tables (see WebArchiveTable in realtime.config).

Every step is repeatable:

- Listings only include copies saved before WEB_ARCHIVE_CUTOFF, after which
  the old website no longer existed, so a listing cannot change once made.
- The archive may hold several saved copies of a file. The earliest version
  (by content hash) is always the one read, so a correction posted later does
  not replace what was first published. Any saved copy of that version will
  do, since they are byte-for-byte the same; one already in the download
  cache is always preferred, so that a rebuild uses exactly the same file.
- Listings, pages and files are all cached as immutable.
"""

import json
import re
import zipfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from itertools import groupby
from urllib.parse import urlencode

import pandas as pd
from loguru import logger
from xlrd import XLRDError

from realtime.cache import CachedFile, DownloadCache, check_is_workbook
from realtime.config import SeriesSpec, WebArchiveSpec, WebArchiveTable
from realtime.http import DownloadError
from realtime.settings import (
    OLD_ONS_RELEASES_PATH,
    WEB_ARCHIVE_INDEX_URL,
    WEB_ARCHIVE_URL,
)
from realtime.sources.base import provenance_columns
from realtime.sources.release_tables import (
    read_workbooks,
    series_in_sheet,
    table_title,
)
from realtime.store import period_ends

ROUTE_NAME = "web_archive"
WEB_ARCHIVE_CUTOFF = "20161231235959"
INDEX_FIELDS = "url,timestamp,digest"
SAVED_OK = "status:200"
RELEASE_PAGE_PATTERNS = (
    re.compile(r"^index\.html$"),
    re.compile(r"^(stb-.*|statistical-bulletin)\.html$"),
)
MAX_RELEASE_PAGE_COPIES = 3
DAY_PRECISION = "day"
UPPER_BOUND_PRECISION = "upper_bound"
MONTH_NAMES = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
]
QUARTERS = range(1, 5)
MONTH_IN_NAME_PATTERN = re.compile(
    r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*-?(\d{4}|\d{2})(?!\d)"
)
QUARTER_IN_NAME_PATTERN = re.compile(r"(q[1-4])-?(\d{4}|\d{2})(?!\d)")
CENTURY = "20"
ISSUED_PATTERN = re.compile(
    rb'DCTERMS\.Issued"[^>]*?content="(\d{4}-\d{2}-\d{2})', re.IGNORECASE
)
HTML_SIGNATURE = b"<html"
SERIES_PERIOD = ["series_id", "reference_period"]
RELATIVE_TOLERANCE = 1e-3
SCALED_DECIMALS = 6
UNREADABLE_WORKBOOK_ERRORS = (ValueError, KeyError, XLRDError, zipfile.BadZipFile)


@dataclass(frozen=True)
class SavedCopy:
    """One copy of a file saved by the web archive.

    Attributes:
        url: The address the file had on the old website.
        timestamp: When the copy was saved, as "YYYYMMDDhhmmss".
        digest: The archive's hash of the copy's content.
    """

    url: str
    timestamp: str
    digest: str

    @property
    def name(self) -> str:
        """The file's name, the last part of its address."""
        return self.url.rsplit("/", 1)[-1]

    @property
    def replay_url(self) -> str:
        """The address of the copy's original bytes in the web archive."""
        return f"{WEB_ARCHIVE_URL}/{self.timestamp}id_/{self.url}"


@dataclass(frozen=True)
class EditionDate:
    """When an edition was published.

    Attributes:
        vintage_date: The publication date.
        precision: "day" if read from the release page, otherwise "upper_bound".
    """

    vintage_date: pd.Timestamp
    precision: str


class WebArchiveSource:
    """Collects vintages from the reference tables of old ONS releases.

    Attributes:
        name: The source code written to the "route" column.
    """

    name = ROUTE_NAME

    def __init__(
        self,
        specs: tuple[WebArchiveSpec, ...],
        series: tuple[SeriesSpec, ...],
        cache: DownloadCache,
    ) -> None:
        """Sets up the source.

        Args:
            specs: The releases to read.
            series: Every configured series, for their bases and labels.
            cache: The download cache.
        """
        self._specs = specs
        self._series = {spec.series_id: spec for spec in series}
        self._cache = cache

    def fetch(self) -> Iterator[pd.DataFrame]:
        """Yields the figures read from each edition of each release.

        Yields:
            One table per edition that holds any configured series.
        """
        for spec in self._specs:
            editions = [
                frame
                for edition in edition_names(spec)
                if not (frame := self._edition_or_nothing(spec, edition)).empty
            ]
            yield from drop_overtaken_editions(editions, spec.release)

    def _edition_or_nothing(self, spec: WebArchiveSpec, edition: str) -> pd.DataFrame:
        """Reads one edition, logging and passing over any download failure.

        Args:
            spec: The release.
            edition: The edition's folder name.

        Returns:
            The edition's figures, or an empty table.
        """
        try:
            return self._edition(spec, edition)
        except DownloadError as error:
            logger.error(f"Skipping {spec.release}/{edition}: {error}")
            return pd.DataFrame()

    def _edition(self, spec: WebArchiveSpec, edition: str) -> pd.DataFrame:
        """Reads the configured series from one edition's reference tables.

        Args:
            spec: The release.
            edition: The edition's folder name, for example "june-2013".

        Returns:
            The figures, with every archive column; empty if the archive holds
            none of the edition's tables.
        """
        copies = [
            copy
            for copy in self._list_copies(spec, edition)
            if not names_another_edition(copy.name, edition)
        ]
        downloads: dict[str, CachedFile | None] = {}
        frames = [
            frame
            for table in spec.tables
            for versions in choose_tables(copies, (table.files,))
            if (cached := self._download_once(versions, downloads))
            for frame in self._figures_in_file(spec, table, cached)
        ]
        files = [cached for cached in downloads.values() if cached is not None]
        if not files:
            if downloads:
                logger.warning(
                    f"{spec.release}/{edition}: no table could be downloaded"
                )
            return pd.DataFrame()
        date = self._edition_date(copies, files)
        figures = self._combine(spec, edition, frames)
        if figures.empty:
            return figures
        if _shows_figures_before_their_time(figures, date):
            logger.error(
                f"{spec.release}/{edition}: figures end on or after "
                f"{date.vintage_date.date()}, its publication date; skipped"
            )
            return pd.DataFrame()
        logger.info(
            f"{spec.release}/{edition}: {len(figures):,} figures for "
            f"{figures['series_id'].nunique()} series, published "
            f"{date.vintage_date.date()} ({date.precision})"
        )
        return figures.assign(
            vintage_date=date.vintage_date, vintage_date_precision=date.precision
        )

    def _list_copies(self, spec: WebArchiveSpec, edition: str) -> list[SavedCopy]:
        """Lists every saved copy of every file in an edition's folder.

        Args:
            spec: The release.
            edition: The edition's folder name.

        Returns:
            The saved copies.
        """
        cached = self._cache.fetch(
            index_url(spec, edition), immutable=True, validate=_check_is_listing
        )
        return parse_listing(cached.content)

    def _download_once(
        self, versions: list[SavedCopy], downloads: dict[str, CachedFile | None]
    ) -> CachedFile | None:
        """Downloads a table's earliest version, unless this edition already has.

        Args:
            versions: Every saved copy of the table.
            downloads: The tables downloaded so far for this edition, by name,
                updated in place.

        Returns:
            The table, or None if it could not be downloaded.
        """
        name = versions[0].name.lower()
        if name not in downloads:
            downloads[name] = self._first_version(versions, check_is_workbook)
        return downloads[name]

    def _first_version(
        self, copies: list[SavedCopy], validate: Callable[[bytes], None]
    ) -> CachedFile | None:
        """Downloads the earliest version of a file, from any copy that works.

        Args:
            copies: Every saved copy of the file.
            validate: A check the downloaded content must pass.

        Returns:
            The file, or None if no copy of its earliest version could be had.
        """
        earliest = min(copies, key=_saved_order)
        same_content = sorted(
            (copy for copy in copies if copy.digest == earliest.digest),
            key=_saved_order,
        )
        return self._first_available(same_content, validate)

    def _first_available(
        self, copies: list[SavedCopy], validate: Callable[[bytes], None]
    ) -> CachedFile | None:
        """Gives the first copy that is already cached, or else the first that downloads.

        Args:
            copies: The copies to try, in order.
            validate: A check the downloaded content must pass.

        Returns:
            The file, or None if no copy could be had.
        """
        ordered = sorted(
            copies, key=lambda copy: not self._cache.is_cached(copy.replay_url)
        )
        for copy in ordered:
            try:
                return self._cache.fetch(
                    copy.replay_url, immutable=True, validate=validate
                )
            except DownloadError as error:
                logger.debug(f"Saved copy unavailable: {error}")
        return None

    def _edition_date(
        self, copies: list[SavedCopy], files: list[CachedFile]
    ) -> EditionDate:
        """Works out when an edition was published.

        Args:
            copies: Every saved copy in the edition's folder.
            files: The tables read from the edition.

        Returns:
            The "DCTERMS.Issued" date on the release page (or, if no copy of
            that can be had, on the statistical bulletin's page), or else the
            day the first table read was saved, as an upper bound.
        """
        for pattern in RELEASE_PAGE_PATTERNS:
            pages = sorted(
                (copy for copy in copies if pattern.match(copy.name)),
                key=_saved_order,
            )
            page = self._first_available(
                pages[:MAX_RELEASE_PAGE_COPIES], _check_is_release_page
            )
            if page is not None:
                return EditionDate(issued_date(page.content), DAY_PRECISION)
        first_saved = min(_saved_day(file.url) for file in files)
        return EditionDate(first_saved, UPPER_BOUND_PRECISION)

    def _combine(
        self, spec: WebArchiveSpec, edition: str, frames: list[pd.DataFrame]
    ) -> pd.DataFrame:
        """Puts together the figures read from an edition's tables.

        Where two tables give the same figure, the one listed first in the
        configuration wins, and any disagreement between them is logged.

        Args:
            spec: The release.
            edition: The edition's folder name.
            frames: The figures read from each table, in order of preference.

        Returns:
            The figures with every archive column except the vintage date.
        """
        if not frames:
            return pd.DataFrame()
        figures = pd.concat(frames, ignore_index=True)
        _log_disagreements(figures, f"{spec.release}/{edition}")
        figures = figures.drop_duplicates(subset=SERIES_PERIOD, keep="first")
        return figures.assign(
            dataset_id=spec.dataset_id,
            short_name=figures["series_id"].map(
                lambda series_id: self._series[series_id].short_name
            ),
            measure=figures["series_id"].map(
                lambda series_id: self._series[series_id].measure
            ),
        ).reset_index(drop=True)

    def _figures_in_file(
        self, spec: WebArchiveSpec, table: WebArchiveTable, cached: CachedFile
    ) -> Iterator[pd.DataFrame]:
        """Reads a table's series from every wanted sheet of a downloaded file.

        Args:
            spec: The release.
            table: Which series to read from the file, and how.
            cached: The downloaded file.

        Yields:
            The figures from each sheet that has any, scaled into the units of
            the series' ONS pages, with their provenance.
        """
        bases = {series_id: self._series[series_id].basis for series_id in table.series}
        name = cached.url.rsplit("/", 1)[-1]
        try:
            for workbook, sheets in read_workbooks(cached.content, name):
                for sheet_name, sheet in sheets.items():
                    title = table_title(sheet)
                    if table.title and not re.search(table.title, title, re.IGNORECASE):
                        continue
                    figures = series_in_sheet(sheet, bases)
                    if not figures.empty:
                        yield figures.assign(
                            value=(figures["value"] * table.scale).round(
                                SCALED_DECIMALS
                            ),
                            title=f"{title or sheet_name} ({workbook})",
                            **provenance_columns(ROUTE_NAME, cached),
                        )
        except UNREADABLE_WORKBOOK_ERRORS as error:
            logger.error(f"{spec.release}: cannot read {cached.url}: {error}")


def drop_overtaken_editions(
    editions: list[pd.DataFrame], release: str
) -> list[pd.DataFrame]:
    """Drops editions dated only by an upper bound that is not before every later edition.

    An edition whose release page could not be had is dated by the day its
    tables were saved, which can be months after it was published. If that
    date is not earlier than a later edition's, the archive would show the
    older edition's figures as the newer ones, so it is left out.

    Args:
        editions: Each edition's figures, in the order the editions came out.
        release: The release, for the message.

    Returns:
        The editions that keep their place in the order.
    """
    dates = [edition["vintage_date"].iloc[0] for edition in editions]
    kept = []
    for position, edition in enumerate(editions):
        is_upper_bound = (
            edition["vintage_date_precision"].iloc[0] == UPPER_BOUND_PRECISION
        )
        overtaken = any(later <= dates[position] for later in dates[position + 1 :])
        if is_upper_bound and overtaken:
            logger.warning(
                f"{release}: an edition dated only by {dates[position].date()} "
                "(the day its tables were saved) is not before a later edition; left out"
            )
            continue
        kept.append(edition)
    return kept


def edition_names(spec: WebArchiveSpec) -> list[str]:
    """Lists the folder names an edition of a release could have.

    Args:
        spec: The release.

    Returns:
        For monthly editions, names such as "january-2011"; for quarterly
        editions, names such as "q1-2011"; for every year from the first year
        to the last, in the order the editions came out.
    """
    years = range(spec.first_year, spec.last_year + 1)
    if spec.editions == "quarter":
        return [f"q{quarter}-{year}" for year in years for quarter in QUARTERS]
    return [f"{month}-{year}" for year in years for month in MONTH_NAMES]


def index_url(spec: WebArchiveSpec, edition: str) -> str:
    """Gives the web archive index query listing an edition's saved files.

    Args:
        spec: The release.
        edition: The edition's folder name.

    Returns:
        The query URL, with its parameters in a fixed order.
    """
    parameters = {
        "url": f"{OLD_ONS_RELEASES_PATH}/{spec.release}/{edition}/*",
        "filter": SAVED_OK,
        "fl": INDEX_FIELDS,
        "output": "json",
        "to": WEB_ARCHIVE_CUTOFF,
    }
    return f"{WEB_ARCHIVE_INDEX_URL}?{urlencode(parameters, safe='/*:,')}"


def parse_listing(content: bytes) -> list[SavedCopy]:
    """Reads the saved copies from a web archive index listing.

    Copies of addresses with a query string (such as "?format=contrast") are
    left out, as they are other renderings of the same file.

    Args:
        content: The listing: one JSON object per line.

    Returns:
        The saved copies.
    """
    entries = [
        json.loads(line)
        for line in content.decode("utf-8").splitlines()
        if line.strip()
    ]
    return [
        SavedCopy(
            url=entry["url"], timestamp=entry["timestamp"], digest=entry["digest"]
        )
        for entry in entries
        if "?" not in entry["url"]
    ]


def choose_tables(
    copies: list[SavedCopy], file_patterns: tuple[str, ...]
) -> list[list[SavedCopy]]:
    """Picks the configured tables from an edition's files, in order of preference.

    Args:
        copies: Every saved copy in the edition's folder.
        file_patterns: Regular expressions for the tables, most preferred first.

    Returns:
        The saved copies of each chosen table, one list per table.
    """
    by_name = {
        name: list(group)
        for name, group in groupby(
            sorted(copies, key=lambda copy: copy.name.lower()),
            key=lambda copy: copy.name.lower(),
        )
    }
    chosen: list[str] = []
    for pattern in file_patterns:
        matcher = re.compile(pattern, re.IGNORECASE)
        chosen += [
            name
            for name in sorted(by_name)
            if matcher.search(name) and name not in chosen
        ]
    return [by_name[name] for name in chosen]


def names_another_edition(name: str, edition: str) -> bool:
    """Checks whether a file's name says it belongs to a different edition.

    Folders sometimes hold a copy of the previous edition's table, for
    example "rft-iop5-nov-2011.xls" in the December 2011 folder.

    Args:
        name: The file name, for example "rft-iop5-nov-2011.xls".
        edition: The edition's folder name, for example "december-2011".

    Returns:
        True if the name gives a month (or quarter) and year, and none of
        them is the edition's own.
    """
    edition_period, edition_year = edition.rsplit("-", 1)
    own_period = (
        edition_period if edition_period.startswith("q") else edition_period[:3]
    )
    named = [
        (period, year if len(year) == 4 else f"{CENTURY}{year}")
        for period, year in MONTH_IN_NAME_PATTERN.findall(name.lower())
        + QUARTER_IN_NAME_PATTERN.findall(name.lower())
    ]
    return bool(named) and (own_period, edition_year) not in named


def issued_date(page: bytes) -> pd.Timestamp:
    """Reads the publication date from an old ONS release page.

    Args:
        page: The page's HTML.

    Returns:
        The date in its "DCTERMS.Issued" metadata.

    Raises:
        ValueError: If the page has no such date.
    """
    match = ISSUED_PATTERN.search(page)
    if match is None:
        raise ValueError("no DCTERMS.Issued date")
    return pd.Timestamp(match[1].decode("ascii"))


def _check_is_listing(content: bytes) -> None:
    """Checks that a download is an index listing, not an error page.

    Args:
        content: The downloaded bytes.

    Raises:
        ValueError: If any line is not a JSON object with an address, time
            and hash.
    """
    if content.lstrip().startswith(b"<"):
        raise ValueError("an HTML page, not an index listing")
    try:
        parse_listing(content)
    except (KeyError, TypeError) as error:
        raise ValueError(f"unexpected index listing: {error}") from error


def _check_is_release_page(content: bytes) -> None:
    """Checks that a download is a release page giving its publication date.

    Args:
        content: The downloaded bytes.

    Raises:
        ValueError: If it is not HTML or has no publication date.
    """
    if HTML_SIGNATURE not in content[:4096].lower():
        raise ValueError("not an HTML page")
    issued_date(content)


def _saved_order(copy: SavedCopy) -> tuple[str, str]:
    """Orders saved copies by when they were saved, then by address.

    Args:
        copy: A saved copy.

    Returns:
        The sort key.
    """
    return copy.timestamp, copy.url


def _saved_day(replay_url: str) -> pd.Timestamp:
    """Gives the day a copy was saved, from its web archive address.

    Args:
        replay_url: An address such as ".../ukgwa/20130903151020id_/http://...".

    Returns:
        Midnight on the day it was saved.
    """
    timestamp = replay_url.removeprefix(f"{WEB_ARCHIVE_URL}/").split("id_", 1)[0]
    return pd.Timestamp(timestamp[:8])


def _shows_figures_before_their_time(figures: pd.DataFrame, date: EditionDate) -> bool:
    """Checks whether any figure's period ends on or after the edition's date.

    Args:
        figures: The edition's figures.
        date: The edition's publication date.

    Returns:
        True if the date must be wrong, since a period cannot be measured
        before it ends.
    """
    return bool((period_ends(figures["reference_period"]) >= date.vintage_date).any())


def _log_disagreements(figures: pd.DataFrame, where: str) -> None:
    """Warns when two tables in one edition give different figures for the same period.

    Args:
        figures: Every figure read from the edition, before duplicates are dropped.
        where: The release and edition, for the message.
    """
    spread = figures.groupby(SERIES_PERIOD)["value"].agg(["min", "max"])
    differs = (spread["max"] - spread["min"]).abs() > RELATIVE_TOLERANCE * spread[
        "max"
    ].abs().clip(lower=1)
    if differs.any():
        examples = spread.loc[differs].head(3).reset_index().to_dict("records")
        logger.warning(
            f"{where}: tables disagree on {int(differs.sum())} figures, for example "
            f"{examples}; the most preferred table is used"
        )
