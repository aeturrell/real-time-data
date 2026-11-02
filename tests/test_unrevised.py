import pandas as pd
import pytest

from realtime.sources.unrevised import latest_publication_date


@pytest.mark.parametrize(
    ("period", "expected"),
    [
        ("1997-01", "1997-02-28"),
        ("1997-12", "1998-01-31"),
        ("1997Q1", "1997-04-30"),
        ("1997", "1998-01-31"),
    ],
)
def test_latest_publication_date_is_end_of_following_month(
    period: str, expected: str
) -> None:
    assert latest_publication_date(pd.Period(period)) == pd.Timestamp(expected)
