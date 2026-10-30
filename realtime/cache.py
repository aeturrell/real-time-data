"""A content-addressed store of every raw file downloaded, with a manifest.

Files are saved under the SHA-256 hash of their contents. The manifest is an
append-only JSON Lines file recording, for every download, the URL, the hash and
when it was retrieved. The archive can therefore be rebuilt from the cache alone.
"""

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from loguru import logger

from realtime.http import DownloadError, HttpClient
from realtime.settings import CACHE_DIR

MANIFEST_FILE_NAME = "manifest.jsonl"
FILES_DIR_NAME = "files"
HASH_PREFIX_LENGTH = 2


class CacheMissError(DownloadError):
    """Raised in offline mode when a URL has never been downloaded."""


@dataclass(frozen=True)
class CachedFile:
    """A downloaded file and the facts needed to trace where it came from.

    Attributes:
        url: The URL the file was downloaded from.
        content: The raw bytes of the file.
        sha256: The SHA-256 hash of the content.
        retrieved_at: When the file was downloaded (UTC).
    """

    url: str
    content: bytes
    sha256: str
    retrieved_at: pd.Timestamp


class DownloadCache:
    """Downloads files through an HTTP client, keeping a permanent copy of each one.

    URLs marked as immutable (past versions on the ONS website, which never change)
    are downloaded once and then always served from the cache. Mutable URLs, such
    as "current" pages, are downloaded again once per run unless working offline.
    """

    def __init__(
        self,
        client: HttpClient | None = None,
        cache_dir: Path = CACHE_DIR,
        offline: bool = False,
    ) -> None:
        """Opens (or creates) a cache directory and reads its manifest.

        Args:
            client: The HTTP client used for downloads. Not needed offline.
            cache_dir: Where cached files and the manifest live.
            offline: If True, never download; serve everything from the cache.
        """
        self._client = client or HttpClient()
        self._files_dir = cache_dir / FILES_DIR_NAME
        self._manifest_path = cache_dir / MANIFEST_FILE_NAME
        self._files_dir.mkdir(parents=True, exist_ok=True)
        self.offline = offline
        self._manifest = self._read_manifest()
        self._fetched_this_run: set[str] = set()

    def fetch(
        self,
        url: str,
        immutable: bool,
        validate: Callable[[bytes], None] | None = None,
    ) -> CachedFile:
        """Returns the file at a URL, downloading it only when necessary.

        Args:
            url: The full URL of the file.
            immutable: Whether the content at this URL can never change.
            validate: Optional check run on new downloads before they are cached;
                it should raise ValueError if the content is unusable.

        Returns:
            The cached file.

        Raises:
            CacheMissError: If offline and the URL has never been downloaded.
            DownloadError: If the download fails or the content fails validation.
        """
        entry = self._manifest.get(url)
        if entry and (immutable or self.offline or url in self._fetched_this_run):
            return self._load(entry)
        if self.offline:
            raise CacheMissError(f"Offline and not cached: {url}")
        content = self._client.get(url).content
        if validate:
            try:
                validate(content)
            except ValueError as error:
                raise DownloadError(f"Unusable content from {url}: {error}") from error
        entry = self._store(url, content)
        self._fetched_this_run.add(url)
        return self._load(entry)

    def fetch_json(self, url: str, immutable: bool) -> dict:
        """Returns the parsed JSON at a URL, downloading it only when necessary.

        Args:
            url: The full URL of the JSON document.
            immutable: Whether the content at this URL can never change.

        Returns:
            The parsed JSON document.
        """
        document, _ = self.fetch_json_with_provenance(url, immutable=immutable)
        return document

    def fetch_json_with_provenance(
        self, url: str, immutable: bool
    ) -> tuple[dict, CachedFile]:
        """Returns the parsed JSON at a URL together with the cached file it came from.

        Args:
            url: The full URL of the JSON document.
            immutable: Whether the content at this URL can never change.

        Returns:
            The parsed JSON document and its cached file.
        """
        cached = self.fetch(url, immutable=immutable, validate=_check_is_json)
        return json.loads(cached.content), cached

    def _read_manifest(self) -> dict[str, dict]:
        """Reads the manifest, keeping the most recent entry for each URL.

        Returns:
            A mapping from URL to its latest manifest entry.
        """
        if not self._manifest_path.exists():
            return {}
        with self._manifest_path.open(encoding="utf-8") as manifest_file:
            entries = (json.loads(line) for line in manifest_file if line.strip())
            return {entry["url"]: entry for entry in entries}

    def _store(self, url: str, content: bytes) -> dict:
        """Saves new content and appends a manifest entry for it.

        Args:
            url: The URL the content came from.
            content: The downloaded bytes.

        Returns:
            The new manifest entry.
        """
        sha256 = hashlib.sha256(content).hexdigest()
        path = self._path_for(sha256)
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
        entry = {
            "url": url,
            "sha256": sha256,
            "retrieved_at": pd.Timestamp.now(tz="UTC").isoformat(),
        }
        with self._manifest_path.open("a", encoding="utf-8") as manifest_file:
            manifest_file.write(json.dumps(entry) + "\n")
        self._manifest[url] = entry
        logger.debug(f"Cached {url} as {sha256[:12]}")
        return entry

    def _load(self, entry: dict) -> CachedFile:
        """Reads a cached file described by a manifest entry.

        Args:
            entry: A manifest entry.

        Returns:
            The cached file.
        """
        return CachedFile(
            url=entry["url"],
            content=self._path_for(entry["sha256"]).read_bytes(),
            sha256=entry["sha256"],
            retrieved_at=pd.Timestamp(entry["retrieved_at"]),
        )

    def _path_for(self, sha256: str) -> Path:
        """Gives the location of a file in the cache from its hash.

        Args:
            sha256: The file's SHA-256 hash.

        Returns:
            The path where the file is (or would be) stored.
        """
        return self._files_dir / sha256[:HASH_PREFIX_LENGTH] / sha256


def _check_is_json(content: bytes) -> None:
    """Checks that downloaded content parses as JSON.

    Args:
        content: The downloaded bytes.

    Raises:
        ValueError: If the content is not valid JSON.
    """
    json.loads(content)
