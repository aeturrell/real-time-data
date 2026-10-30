import numpy as np
import pandas as pd
import pytest

from realtime.sources.realtime_db import read_database, vintage_month_of_label

NAN = np.nan


@pytest.mark.parametrize(
    ("label", "expected"),
    [
        ("Sep-61 [1954 prices]", "1961-09"),
        ("Dec-03 [2000 prices]\nQNA", "2003-12"),
        ("Aug- 26 1st ", "2026-08"),
        ("June-2018 \nQNA", "2018-06"),
        ("Jun- 2026 QNA", "2026-06"),
        ("1998 Q3 M2 ", "1998-11"),
        ("2026 Q2 [2024 prices] QNA", "2026-09"),
        ("Q4 1989\nM1 ", "1990-01"),
        ("Q1 2000\n QNA", "2000-06"),
        ("2026 Q2 1st", "2026-08"),
        ("Jan to Mar 2011 (P) 2007 prices", "2011-05"),
        ("Apr to Jun 2026 (R) 2024 prices", "2026-09"),
    ],
)
def test_vintage_month_of_label(label: str, expected: str) -> None:
    assert vintage_month_of_label(label) == pd.Period(expected, "M")


@pytest.mark.parametrize(
    "label", ["Q1 1955", "1970 Q1", "Publication date and time period", 870.0, None]
)
def test_vintage_month_of_label_rejects_other_cells(label: object) -> None:
    assert vintage_month_of_label(label) is None


def test_read_database_takes_columns_as_vintages_and_rows_as_quarters() -> None:
    sheet = pd.DataFrame(
        [
            ["Real-time database for GDP", NAN, NAN],
            ["Publication date and time period", "1998 Q3 M2 ", "1998 Q3 QNA"],
            ["Q2 1998", 100.0, 101.0],
            ["Q3 1998", NAN, "*"],
            ["Q4 1998", NAN, NAN],
        ]
    )
    long = read_database(sheet)
    assert len(long) == 2
    assert set(long["vintage_month"]) == {pd.Period("1998-11"), pd.Period("1998-12")}
    assert set(long["reference_period"]) == {pd.Period("1998Q2", "Q-DEC")}
