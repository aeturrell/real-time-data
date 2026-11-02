from datetime import datetime

import numpy as np
import pandas as pd

from realtime.sources.triangle import (
    EditionVersion,
    _version_live_before,
    parse_triangle,
)

NAN = np.nan


def test_parses_datetime_layout_and_ignores_summary_rows() -> None:
    sheet = pd.DataFrame(
        [
            [NAN, "Title", NAN, NAN],
            [NAN, "Relating to Period", datetime(1999, 1, 1), datetime(1999, 2, 1)],
            [NAN, "Month 1 estimate", 1.0, NAN],
            [NAN, "First Estimate", 1.0, 2.0],
            [NAN, datetime(1999, 2, 1), 1.0, NAN],
            [NAN, datetime(1999, 3, 1), 1.1, 2.0],
            [NAN, "Latest Estimate", 1.1, 2.0],
        ]
    )
    long = parse_triangle(sheet, "M")
    assert len(long) == 3
    assert set(long["vintage_month"]) == {pd.Period("1999-02"), pd.Period("1999-03")}


def test_parses_text_quarterly_layout_with_summary_table_above() -> None:
    sheet = pd.DataFrame(
        [
            ["Table 1", NAN, NAN],
            ["Relating to Period", "1998 Q1", "1998 Q2"],
            ["Provisional estimate", "[x]", 2.7],
            [NAN, NAN, NAN],
            ["Time period row and publication data column", "1998 Q1", "1998 Q2"],
            ["Jun-98", 5.8, "[x]"],
            ["Sep-98", 5.0, -2.7],
            ["Dec-98", 5.0, -2.5],
        ]
    )
    long = parse_triangle(sheet, "Q")
    assert long["vintage_month"].nunique() == 3
    assert set(long["reference_period"]) == {pd.Period("1998Q1"), pd.Period("1998Q2")}
    assert len(long) == 5


def test_parses_index_header_with_two_digit_years() -> None:
    sheet = pd.DataFrame(
        [
            ["Index", "Jan-98", "Feb-98"],
            ["First estimate", 18035, 18067],
            ["Mar-98", 18035, ""],
            ["Apr-98", 18003, 18067],
        ]
    )
    long = parse_triangle(sheet, "M")
    assert long.loc[
        long["vintage_month"] == pd.Period("1998-04"), "value"
    ].tolist() == [18003, 18067]


VERSIONS = [
    EditionVersion("/x/previous/v1", pd.Timestamp("2015-11-19")),
    EditionVersion("/x/previous/v2", pd.Timestamp("2015-12-17")),
    EditionVersion("/x/previous/v3", pd.Timestamp("2016-01-21")),
    EditionVersion("/x", None),
]


def test_next_version_is_the_one_live_just_before_the_earliest_month() -> None:
    assert _version_live_before(VERSIONS, pd.Period("2016-01", "M"), 3) == 2
    assert _version_live_before(VERSIONS, pd.Period("2015-12", "M"), 3) == 1
    assert _version_live_before(VERSIONS, pd.Period("2025-01", "M"), 3) == 2


def test_next_version_is_always_older_and_stops_after_the_first() -> None:
    assert _version_live_before(VERSIONS, pd.Period("1998-01", "M"), 3) == 0
    assert _version_live_before(VERSIONS, pd.Period("1998-01", "M"), 0) is None
