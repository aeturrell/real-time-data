import pandas as pd

from realtime.benchmark import Benchmark, _compare_edition, _growth

QUARTERS = pd.period_range("2024Q1", "2024Q4", freq="Q-DEC")
BENCHMARK = Benchmark("GDP", "B1GQ_Q", "Q", "ABMI", False, 1)


def test_growth_works_along_periods() -> None:
    levels = pd.DataFrame([[100.0, 101.0, 103.02]], columns=QUARTERS[:3])
    growth = _growth(levels, 1).iloc[0]
    assert pd.isna(growth.iloc[0])
    assert round(growth.iloc[1], 6) == 1.0
    assert round(growth.iloc[2], 6) == 2.0


def test_compare_edition_finds_strict_and_nearby_differences() -> None:
    ours = pd.DataFrame(
        [[0.5, 0.2], [0.5, 0.3]],
        index=pd.DatetimeIndex(["2024-09-30", "2024-10-15"]),
        columns=QUARTERS[1:3],
    )
    theirs = pd.Series([0.5, 0.3], index=QUARTERS[1:3])
    results = _compare_edition(ours, theirs, pd.Timestamp("2024-10-01"), BENCHMARK)
    by_period = {result["period"]: result for result in results}
    assert round(by_period[QUARTERS[2]]["strict"], 6) == 0.1
    assert by_period[QUARTERS[2]]["nearby"] == 0.0
    assert by_period[QUARTERS[1]]["strict"] == 0.0


def test_benchmark_covers_only_its_editions() -> None:
    limited = Benchmark("CPI", "CP", "M", "D7G7", True, 12, "", "2008-01", "2017-12")
    assert limited.covers(pd.Timestamp("2010-06-01"))
    assert not limited.covers(pd.Timestamp("2019-01-01"))
    assert BENCHMARK.covers(pd.Timestamp("1999-02-01"))
