import pandas as pd

from realtime.validate import late_first_appearances, mixed_basis_summary


def row(
    period: str, vintage: str, source: str = "series_json", basis: str = "period"
) -> dict:
    return {
        "series_id": "MGSX",
        "frequency": "Q" if "Q" in period else "M",
        "reference_period": period,
        "reference_period_basis": basis,
        "vintage_date": pd.Timestamp(vintage),
        "source": source,
        "value": 1.0,
    }


def test_late_first_appearances_finds_releases_a_source_had_earlier() -> None:
    observations = pd.DataFrame(
        [
            row("2015Q2", "2015-07-31", source="triangle"),
            row("2015Q2", "2015-09-30"),
            row("1990Q1", "2015-05-31", source="triangle"),
            row("1990Q1", "2016-02-25"),
        ]
    )
    changes = observations.loc[observations["source"] == "series_json"]
    late = late_first_appearances(observations, changes)
    assert late["reference_period"].tolist() == ["2015Q2"]
    assert late["days_late"].tolist() == [61]


def test_mixed_basis_summary_flags_series_with_two_bases() -> None:
    archive = pd.DataFrame(
        [
            row("2015-07", "2015-10-14"),
            row("2015-08", "2015-12-16", basis="3m_ending"),
            row("2015Q2", "2015-10-14"),
        ]
    )
    mixed = mixed_basis_summary(archive)
    assert sorted(mixed["reference_period_basis"]) == ["3m_ending", "period"]
    assert mixed_basis_summary(archive.iloc[1:]).empty


def test_late_first_appearances_is_empty_when_nothing_is_late() -> None:
    observations = pd.DataFrame([row("2015Q2", "2015-07-31")])
    assert late_first_appearances(observations, observations).empty
