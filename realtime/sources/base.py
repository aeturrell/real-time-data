"""The interface every data source follows, and helpers they share."""

from collections.abc import Iterator
from typing import Protocol

import pandas as pd

from realtime.cache import CachedFile

SOURCE_PRIORITY = {
    "series_json": 0,
    "dataset_csv": 0,
    "ons_api": 1,
    "triangle": 2,
    "unrevised": 2,
    "realtime_db": 3,
    "web_archive": 4,
}


class Source(Protocol):
    """A source of vintages.

    Attributes:
        name: The source code written to the "source" column.
    """

    name: str

    def fetch(self) -> Iterator[pd.DataFrame]:
        """Yields tables of observations, each already collapsed to changes.

        Yields:
            Tables with every archive column, reference periods as strings.
        """
        ...


def provenance_columns(source: str, cached: CachedFile) -> dict[str, object]:
    """Gives the columns recording where and when a set of values was obtained.

    Args:
        source: The source code.
        cached: The cached file the values came from.

    Returns:
        Column names mapped to values.
    """
    return {
        "source": source,
        "source_url": cached.url,
        "retrieved_at": cached.retrieved_at,
        "content_sha256": cached.sha256,
    }
