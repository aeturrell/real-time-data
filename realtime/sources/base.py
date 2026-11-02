"""The interface every collection route follows, and helpers they share."""

from collections.abc import Iterator
from typing import Protocol

import pandas as pd

from realtime.cache import CachedFile

ONS_SOURCE = "ONS"
BANK_OF_ENGLAND_SOURCE = "Bank of England"

ROUTE_PRIORITY = {
    "series_json": 0,
    "dataset_csv": 0,
    "ons_api": 1,
    "triangle": 2,
    "unrevised": 2,
    "realtime_db": 3,
    "web_archive": 4,
    "ons_bics": 0,
    "boe_dmp": 0,
    "boe_ccs": 0,
    "boe_agents": 0,
}


class Source(Protocol):
    """A route by which vintages are collected.

    Attributes:
        name: The route code written to the "route" column.
    """

    name: str

    def fetch(self) -> Iterator[pd.DataFrame]:
        """Yields tables of observations, each already collapsed to changes.

        Yields:
            Tables with every archive column, reference periods as strings.
        """
        ...


def provenance_columns(
    route: str, cached: CachedFile, source: str = ONS_SOURCE
) -> dict[str, object]:
    """Gives the columns recording where and when a set of values was obtained.

    Args:
        route: The route code, which says how the values were collected.
        cached: The cached file the values came from.
        source: The organisation that published the values.

    Returns:
        Column names mapped to values.
    """
    return {
        "source": source,
        "route": route,
        "source_url": cached.url,
        "retrieved_at": cached.retrieved_at,
        "content_sha256": cached.sha256,
    }
