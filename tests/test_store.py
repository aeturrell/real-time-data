from pathlib import Path

import pandas as pd
import pytest

from realtime.store import (
    as_of,
    collapse_to_changes,
    convert_frequency,
    prefer_best_sources,
    read_archive,
    revision_triangle,
    to_period_dtype,
    write_archive,
)

RETRIEVED_AT = pd.Timestamp("2026-10-29", tz="UTC")


def observation(
    period: str, vintage: str, value: float, series_id: str = "ABMI", rank: int = 0
) -> dict:
    frequency = "Q" if "Q" in period else ("M" if "-" in period else "A")
    return {
        "series_id": series_id,
        "dataset_id": "QNA",
        "title": "GDP",
        "short_name": "gdp",
        "measure": "£m",
        "frequency": frequency,
        "reference_period": period,
        "reference_period_basis": "period",
        "vintage_date": pd.Timestamp(vintage),
        "vintage_date_precision": "day",
        "value": value,
        "source": "series_json",
        "source_url": "https://example.org",
        "retrieved_at": RETRIEVED_AT,
        "content_sha256": "abc",
        "_rank": rank,
        "_source_rank": rank // 2,
    }


def test_collapse_keeps_first_publications_and_revisions_only() -> None:
    rows = pd.DataFrame(
        [
            observation("2024Q1", "2024-05-10", 100.0),
            observation("2024Q1", "2024-06-28", 100.0),
            observation("2024Q1", "2024-09-30", 101.0),
            observation("2024Q2", "2024-09-30", 102.0),
            observation("2024Q1", "2024-12-20", 101.0 + 1e-12),
        ]
    )
    changes = collapse_to_changes(rows)
    assert list(
        zip(
            changes["reference_period"],
            changes["vintage_date"].dt.strftime("%Y-%m-%d"),
            strict=True,
        )
    ) == [
        ("2024Q1", "2024-05-10"),
        ("2024Q1", "2024-09-30"),
        ("2024Q2", "2024-09-30"),
    ]


def test_collapse_prefers_lower_rank_then_later_row_on_same_key() -> None:
    rows = pd.DataFrame(
        [
            observation("2024Q1", "2024-05-10", 1.0, rank=1),
            observation("2024Q1", "2024-05-10", 2.0, rank=0),
            observation("2024Q1", "2024-05-10", 3.0, rank=0),
        ]
    )
    assert collapse_to_changes(rows)["value"].tolist() == [3.0]


def test_prefer_best_sources_drops_weaker_rows_once_stronger_source_starts() -> None:
    rows = pd.DataFrame(
        [
            observation("2014Q1", "2014-05-31", 1.0, rank=4),
            observation("2014Q1", "2016-03-31", 1.1, rank=4),
            observation("2014Q1", "2016-03-31", 1.12, rank=0),
        ]
    )
    kept = prefer_best_sources(rows)
    assert kept["value"].tolist() == [1.0, 1.12]


def test_prefer_best_sources_lets_each_source_fill_only_before_stronger_ones() -> None:
    rows = pd.DataFrame(
        [
            observation("1990Q1", "1990-05-31", 1.0, rank=6),
            observation("1990Q1", "1996-05-31", 1.1, rank=6),
            observation("1990Q1", "1996-05-31", 1.2, rank=4),
            observation("1990Q1", "2016-03-31", 1.3, rank=4),
            observation("1990Q1", "2016-03-31", 1.4, rank=0),
        ]
    )
    assert prefer_best_sources(rows)["value"].tolist() == [1.0, 1.2, 1.4]


def test_prefer_best_sources_keeps_other_datasets_of_the_best_source() -> None:
    rows = pd.DataFrame(
        [
            observation("2024Q1", "2024-05-10", 1.0, rank=1),
            observation("2024Q1", "2024-06-28", 1.1, rank=0),
            observation("2024Q2", "2024-08-12", 2.0, rank=1),
        ]
    )
    assert prefer_best_sources(rows)["value"].tolist() == [1.0, 1.1, 2.0]


@pytest.fixture
def archive(tmp_path: Path) -> pd.DataFrame:
    rows = pd.DataFrame(
        [
            observation("2024Q1", "2024-05-10", 100.0),
            observation("2024Q1", "2024-09-30", 101.0),
            observation("2024Q2", "2024-09-30", 102.0),
            observation("2024-03", "2024-04-15", 5.0, series_id="LF24"),
        ]
    ).drop(columns=["_rank", "_source_rank"])
    path = tmp_path / "archive.parquet"
    write_archive(rows, path)
    return read_archive(path)


def test_read_archive_restores_mixed_frequency_periods(archive: pd.DataFrame) -> None:
    assert set(archive["reference_period"]) == {
        pd.Period("2024Q1"),
        pd.Period("2024Q2"),
        pd.Period("2024-03"),
    }


def test_as_of_rebuilds_a_past_vintage(archive: pd.DataFrame) -> None:
    gdp = as_of(archive, "2024-06-01").query("series_id == 'ABMI'")
    assert gdp["value"].tolist() == [100.0]
    later = (
        as_of(archive, "2024-10-01")
        .query("series_id == 'ABMI'")
        .sort_values("reference_period")
    )
    assert later["value"].tolist() == [101.0, 102.0]


def test_revision_triangle_carries_values_forward(archive: pd.DataFrame) -> None:
    triangle = revision_triangle(archive, "ABMI")
    assert triangle.loc[pd.Timestamp("2024-09-30"), pd.Period("2024Q1")] == 101.0
    assert pd.isna(triangle.loc[pd.Timestamp("2024-05-10"), pd.Period("2024Q2")])


def test_convert_frequency_and_period_dtype(archive: pd.DataFrame) -> None:
    quarters = convert_frequency(archive["reference_period"], "Q")
    assert set(quarters) == {pd.Period("2024Q1"), pd.Period("2024Q2")}
    monthly = archive.loc[archive["frequency"] == "M", "reference_period"]
    assert isinstance(to_period_dtype(monthly).dtype, pd.PeriodDtype)
    with pytest.raises(ValueError):
        to_period_dtype(archive["reference_period"])
