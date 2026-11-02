"""Finding and opening the spreadsheet files attached to ONS dataset pages."""

import io
import re
import zipfile

import pandas as pd
from openpyxl.utils.exceptions import InvalidFileException
from xlrd import XLRDError

from realtime.cache import CachedFile, DownloadCache
from realtime.http import DownloadError
from realtime.settings import ONS_BASE_URL

ZIP_EXTENSION = ".zip"
WORKBOOK_EXTENSIONS = (".xlsx", ".xlsm", ".xls")
DOWNLOAD_EXTENSIONS = (*WORKBOOK_EXTENSIONS, ZIP_EXTENSION)
PREVIOUS_VERSION_MARKER = "/previous/v"
READ_ERRORS = (
    DownloadError,
    KeyError,
    ValueError,
    zipfile.BadZipFile,
    XLRDError,
    InvalidFileException,
)


def latest_edition(cache: DownloadCache, dataset_path: str) -> str:
    """Finds the newest edition of a dataset page.

    Args:
        cache: The download cache.
        dataset_path: The dataset landing page path.

    Returns:
        The path of the newest edition (the landing page itself if it has no editions).
    """
    landing = cache.fetch_json(f"{ONS_BASE_URL}{dataset_path}/data", immutable=False)
    editions = landing.get("datasets") or []
    return editions[0]["uri"] if editions else dataset_path


def download_spreadsheet(
    cache: DownloadCache, edition_uri: str, file_pattern: str | None
) -> CachedFile:
    """Downloads the spreadsheet from an edition page or one of its past versions.

    Past versions never change, so they are downloaded only once.

    Args:
        cache: The download cache.
        edition_uri: The edition (or past version) page path.
        file_pattern: Optional regular expression picking the file from the downloads.

    Returns:
        The cached file.

    Raises:
        ValueError: If no suitable download is listed.
    """
    immutable = PREVIOUS_VERSION_MARKER in edition_uri
    edition = cache.fetch_json(f"{ONS_BASE_URL}{edition_uri}/data", immutable=immutable)
    files = [download["file"] for download in edition.get("downloads", [])]
    candidates = [name for name in files if name.lower().endswith(DOWNLOAD_EXTENSIONS)]
    if file_pattern:
        candidates = [
            name for name in candidates if re.search(file_pattern, name, re.IGNORECASE)
        ]
    if not candidates:
        raise ValueError(f"No matching download in {edition_uri}: {files}")
    return cache.fetch(
        f"{ONS_BASE_URL}/file?uri={edition_uri}/{candidates[0]}",
        immutable=immutable,
    )


def open_workbook(
    content: bytes, url: str, member_pattern: str | None
) -> dict[str, pd.DataFrame]:
    """Opens a spreadsheet, or the matching spreadsheet inside a zip file.

    Args:
        content: The downloaded bytes.
        url: Where the file came from; a ".zip" ending marks a zip download.
        member_pattern: Regular expression picking the file inside a zip.

    Returns:
        Every worksheet, read with no header, keyed by name.

    Raises:
        ValueError: If a zip does not contain exactly one matching spreadsheet.
    """
    if url.lower().endswith(ZIP_EXTENSION):
        content = extract_member(content, member_pattern)
    return pd.read_excel(io.BytesIO(content), sheet_name=None, header=None)


def extract_member(content: bytes, member_pattern: str | None) -> bytes:
    """Pulls one spreadsheet out of a zip file.

    Args:
        content: The zip file's bytes.
        member_pattern: Regular expression picking the file.

    Returns:
        The spreadsheet's bytes.

    Raises:
        ValueError: If there is not exactly one match.
    """
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        names = [
            name
            for name in archive.namelist()
            if name.lower().endswith(WORKBOOK_EXTENSIONS)
        ]
        if member_pattern:
            names = [
                name for name in names if re.search(member_pattern, name, re.IGNORECASE)
            ]
        if len(names) != 1:
            raise ValueError(f"Expected one matching file in zip, found {names}")
        return archive.read(names[0])


def pick_sheet(workbook: dict[str, pd.DataFrame], sheet_pattern: str) -> pd.DataFrame:
    """Picks the worksheet holding the triangle.

    Args:
        workbook: Worksheets keyed by name.
        sheet_pattern: Regular expression matching the sheet name.

    Returns:
        The first matching worksheet.

    Raises:
        ValueError: If none match.
    """
    matches = [
        name for name in workbook if re.search(sheet_pattern, name, re.IGNORECASE)
    ]
    if not matches:
        raise ValueError(f"No sheet matching {sheet_pattern!r} in {list(workbook)}")
    return workbook[matches[0]]


def pick_sheets(
    workbook: dict[str, pd.DataFrame], sheet_pattern: str
) -> list[pd.DataFrame]:
    """Picks every worksheet whose name matches a pattern, in workbook order.

    Args:
        workbook: Worksheets keyed by name.
        sheet_pattern: Regular expression matching the sheet names.

    Returns:
        The matching worksheets.

    Raises:
        ValueError: If none match.
    """
    matches = [
        frame
        for name, frame in workbook.items()
        if re.search(sheet_pattern, name, re.IGNORECASE)
    ]
    if not matches:
        raise ValueError(f"No sheet matching {sheet_pattern!r} in {list(workbook)}")
    return matches
