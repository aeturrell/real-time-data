"""Finding Bank of England publication pages, their true dates and their files.

Every page is found through the Bank's public sitemap. The date in a page's
"Published on" text is the true publication date; the date in the page's hidden
metadata is not used, because it often shows when the page was last rebuilt. The
sitemap also lists releases that are scheduled but not yet out, and these are
recognised by comparing their date with the day the page was downloaded.
"""

import html
import re
from collections.abc import Iterator
from dataclasses import dataclass

import pandas as pd
from loguru import logger

from realtime.cache import CachedFile, DownloadCache, check_is_workbook
from realtime.http import DownloadError
from realtime.settings import BANK_OF_ENGLAND_BASE_URL

SITEMAP_URL = f"{BANK_OF_ENGLAND_BASE_URL}/_api/sitemap/getsitemap"
PUBLISHED_PATTERN = re.compile(
    r"Published\s+on\s*(?:<[^>]*>\s*)*(\d{1,2}\s+[A-Za-z]+\s+\d{4})"
)
SPREADSHEET_LINK_PATTERN = re.compile(r'href="([^"]+\.xlsx?)"', re.IGNORECASE)
PAGE_YEAR_POSITION = 2


@dataclass(frozen=True)
class BankPage:
    """One published page on the Bank of England website.

    Attributes:
        path: The page's path, for example "/agents-summary/2012/january-2012".
        published: The date in the page's "Published on" text.
        is_released: Whether that date had arrived when the page was downloaded.
        spreadsheets: The full addresses of the spreadsheets the page links to.
    """

    path: str
    published: pd.Timestamp
    is_released: bool
    spreadsheets: tuple[str, ...]

    @property
    def slug(self) -> str:
        """Gives the last part of the page's path, such as "2018-q2"."""
        return self.path.rsplit("/", 1)[-1]


def section_paths(cache: DownloadCache, section: str) -> list[str]:
    """Lists the pages the sitemap holds for one section of the website.

    Args:
        cache: The download cache.
        section: The section, for example "credit-conditions-survey".

    Returns:
        The page paths, sorted.
    """
    sitemap = cache.fetch(SITEMAP_URL, immutable=False).content.decode("utf-8")
    pattern = rf"/{re.escape(section)}/\d{{4}}/[a-z0-9-]+"
    return sorted(set(re.findall(pattern, sitemap)))


def read_page(cache: DownloadCache, path: str) -> BankPage:
    """Downloads a page if needed and reads its date and spreadsheet links.

    Pages from earlier years are downloaded once. Pages from this year, and any
    page that was not yet released when last downloaded, are checked again.

    Args:
        cache: The download cache.
        path: The page's path.

    Returns:
        The page.

    Raises:
        ValueError: If the page shows no "Published on" date.
    """
    settled = _page_year(path) < pd.Timestamp.now().year
    page = _parse_page(path, cache.fetch(_page_url(path), immutable=settled))
    if settled and not page.is_released and not cache.offline:
        page = _parse_page(path, cache.fetch(_page_url(path), immutable=False))
    return page


def released_pages(cache: DownloadCache, section: str) -> Iterator[BankPage]:
    """Yields the released pages of a section, skipping any that cannot be read.

    Args:
        cache: The download cache.
        section: The section of the website.

    Yields:
        Each released page, oldest address first.
    """
    for path in section_paths(cache, section):
        try:
            page = read_page(cache, path)
        except (DownloadError, ValueError) as error:
            logger.error(f"Skipping Bank of England page {path}: {error}")
            continue
        if page.is_released:
            yield page


def download_spreadsheet(cache: DownloadCache, url: str) -> CachedFile:
    """Downloads a spreadsheet whose address belongs to one release.

    Args:
        cache: The download cache.
        url: The full address, read from a page and never guessed.

    Returns:
        The cached file.
    """
    return cache.fetch(url, immutable=True, validate=check_is_workbook)


def _page_url(path: str) -> str:
    """Gives a page's full address.

    Args:
        path: The page's path.

    Returns:
        The full address.
    """
    return f"{BANK_OF_ENGLAND_BASE_URL}{path}"


def _page_year(path: str) -> int:
    """Reads the year from a page's path.

    Args:
        path: The page's path, for example "/agents-summary/2012/january-2012".

    Returns:
        The year.
    """
    return int(path.split("/")[PAGE_YEAR_POSITION])


def _parse_page(path: str, cached: CachedFile) -> BankPage:
    """Reads the facts needed from a downloaded page.

    Args:
        path: The page's path.
        cached: The downloaded page.

    Returns:
        The page.
    """
    text = cached.content.decode("utf-8", errors="replace")
    published = parse_published_date(text)
    return BankPage(
        path=path,
        published=published,
        is_released=published <= cached.retrieved_at.tz_localize(None).normalize(),
        spreadsheets=tuple(spreadsheet_links(text)),
    )


def parse_published_date(page_html: str) -> pd.Timestamp:
    """Finds the date in a page's "Published on" text.

    Args:
        page_html: The page's HTML.

    Returns:
        The publication date.

    Raises:
        ValueError: If the page shows no such date.
    """
    found = PUBLISHED_PATTERN.search(page_html)
    if not found:
        raise ValueError("no 'Published on' date found")
    return pd.Timestamp(found.group(1))


def spreadsheet_links(page_html: str) -> list[str]:
    """Lists the spreadsheets a page links to, without repeats.

    Args:
        page_html: The page's HTML.

    Returns:
        Full addresses, in the order the page gives them.
    """
    links = [html.unescape(link) for link in SPREADSHEET_LINK_PATTERN.findall(page_html)]
    full_links = [
        link if link.startswith("http") else f"{BANK_OF_ENGLAND_BASE_URL}{link}"
        for link in links
    ]
    return list(dict.fromkeys(full_links))
