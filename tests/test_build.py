import pandas as pd

from realtime.build import add_rank
from realtime.sources.base import ROUTE_PRIORITY, WEAKEST_PRIORITY


def rows(*routes_and_precisions: tuple[str, str]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "series_id": "ABMI",
                "dataset_id": "QNA",
                "route": route,
                "vintage_date_precision": precision,
            }
            for route, precision in routes_and_precisions
        ]
    )


def test_an_old_release_dated_only_by_an_upper_bound_ranks_weakest() -> None:
    ranked = add_rank(
        rows(
            ("web_archive", "day"),
            ("web_archive", "upper_bound"),
            ("series_json", "upper_bound"),
        ),
        {"ABMI": "QNA"},
    )
    assert ranked["_source_rank"].tolist() == [
        ROUTE_PRIORITY["web_archive"],
        WEAKEST_PRIORITY,
        ROUTE_PRIORITY["series_json"],
    ]
