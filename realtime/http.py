"""A polite HTTP client: rate limited for each website, with retries and back-off."""

import time
from urllib.parse import urlsplit

import requests
from loguru import logger

from realtime.settings import USER_AGENT

MIN_SECONDS_BETWEEN_REQUESTS = 1.5
BANK_OF_ENGLAND_HOST = "www.bankofengland.co.uk"
BANK_OF_ENGLAND_SECONDS_BETWEEN_REQUESTS = 3.0
SECONDS_BETWEEN_REQUESTS_BY_HOST = {
    BANK_OF_ENGLAND_HOST: BANK_OF_ENGLAND_SECONDS_BETWEEN_REQUESTS
}
REQUEST_TIMEOUT_SECONDS = 60
MAX_ATTEMPTS = 6
BACKOFF_BASE_SECONDS = 5.0
RATE_LIMIT_BACKOFF_SECONDS = 60.0
HTTP_NOT_FOUND = 404
HTTP_TOO_MANY_REQUESTS = 429
RETRYABLE_STATUS_CODES = frozenset({HTTP_TOO_MANY_REQUESTS, 500, 502, 503, 504})


class DownloadError(Exception):
    """Raised when a file cannot be downloaded after every retry."""


class NotFoundError(DownloadError):
    """Raised when the server says the requested URL does not exist."""


class HttpClient:
    """Fetches URLs one at a time, leaving a minimum gap between requests to a website.

    Attributes:
        min_interval_seconds: Shortest allowed gap between two requests to a
            website with no gap of its own in SECONDS_BETWEEN_REQUESTS_BY_HOST.
    """

    def __init__(
        self, min_interval_seconds: float = MIN_SECONDS_BETWEEN_REQUESTS
    ) -> None:
        """Initialises the client with a shared session and an honest user agent.

        Args:
            min_interval_seconds: Shortest allowed gap between two requests to a
                website with no gap of its own.
        """
        self.min_interval_seconds = min_interval_seconds
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": USER_AGENT})
        self._last_request_at: dict[str, float] = {}

    def get(self, url: str) -> requests.Response:
        """Downloads a URL, retrying on rate limits, server errors and dropped connections.

        Args:
            url: The full URL to fetch.

        Returns:
            The successful response.

        Raises:
            NotFoundError: If the server returns 404.
            DownloadError: If every attempt fails.
        """
        for attempt in range(1, MAX_ATTEMPTS + 1):
            self._wait_for_turn(urlsplit(url).netloc)
            try:
                response = self._session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
            except requests.RequestException as error:
                logger.warning(f"Attempt {attempt} for {url} failed: {error}")
                time.sleep(BACKOFF_BASE_SECONDS * attempt)
                continue
            if response.status_code == HTTP_NOT_FOUND:
                raise NotFoundError(f"Not found: {url}")
            if response.status_code in RETRYABLE_STATUS_CODES:
                wait_seconds = self._retry_wait_seconds(response, attempt)
                logger.warning(
                    f"Attempt {attempt} for {url} returned {response.status_code}; "
                    f"waiting {wait_seconds:.0f}s"
                )
                time.sleep(wait_seconds)
                continue
            if not response.ok:
                raise DownloadError(f"{url} returned status {response.status_code}")
            return response
        raise DownloadError(f"Giving up on {url} after {MAX_ATTEMPTS} attempts")

    def _wait_for_turn(self, host: str) -> None:
        """Sleeps until the minimum gap since the previous request to a website has passed.

        Args:
            host: The website about to be contacted.
        """
        interval = SECONDS_BETWEEN_REQUESTS_BY_HOST.get(host, self.min_interval_seconds)
        elapsed = time.monotonic() - self._last_request_at.get(host, 0.0)
        if elapsed < interval:
            time.sleep(interval - elapsed)
        self._last_request_at[host] = time.monotonic()

    @staticmethod
    def _retry_wait_seconds(response: requests.Response, attempt: int) -> float:
        """Works out how long to wait before retrying a failed request.

        Args:
            response: The failed response.
            attempt: Which attempt (starting at 1) just failed.

        Returns:
            Seconds to wait: the longer of any numeric Retry-After header and a
            back-off that grows with each attempt.
        """
        retry_after = response.headers.get("Retry-After", "")
        requested = float(retry_after) if retry_after.isdigit() else 0.0
        base = (
            RATE_LIMIT_BACKOFF_SECONDS
            if response.status_code == HTTP_TOO_MANY_REQUESTS
            else BACKOFF_BASE_SECONDS
        )
        return max(requested, base * attempt)
