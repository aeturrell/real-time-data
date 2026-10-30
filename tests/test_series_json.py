import pandas as pd

from realtime.cache import CachedFile
from realtime.config import SeriesSpec
from realtime.release_dates import Vintage
from realtime.sources.series_json import (
    dataset_of_page,
    latest_period_end,
    observations_from_document,
)

SPEC = SeriesSpec(
    series_id="LF24",
    short_name="employment_rate",
    measure="Percent",
    primary_dataset="LMS",
)
CACHED = CachedFile(
    url="https://www.ons.gov.uk/x/timeseries/lf24/lms/data",
    content=b"{}",
    sha256="abc",
    retrieved_at=pd.Timestamp("2026-10-29", tz="UTC"),
)
VINTAGE = Vintage("current", pd.Timestamp("2026-10-20"), "day")
DOCUMENT = {
    "uri": "/x/timeseries/lf24/lms",
    "description": {"title": "Employment rate ", "datasetId": "LMS"},
    "years": [{"date": "2025", "label": "2025", "value": "75.1"}],
    "quarters": [{"date": "2026 Q2", "label": "2026 Q2", "value": "75.1"}],
    "months": [
        {"date": "2026 JUL", "label": "2026 JUN-AUG", "value": "75.3"},
        {"date": "2026 AUG", "label": "2026 JUL-SEP", "value": ""},
    ],
}


def test_observations_from_document() -> None:
    rows = observations_from_document(DOCUMENT, CACHED, VINTAGE, SPEC).set_index(
        "reference_period"
    )
    assert sorted(rows.index) == ["2025", "2026-08", "2026Q2"]
    assert rows.loc["2026-08", "reference_period_basis"] == "3m_ending"
    assert rows.loc["2026Q2", "reference_period_basis"] == "period"
    assert rows.loc["2025", "frequency"] == "A"
    assert rows.loc["2026-08", "value"] == 75.3
    assert rows["title"].unique().tolist() == ["Employment rate"]
    assert (rows["vintage_date"] == VINTAGE.vintage_date).all()


def test_dataset_of_page() -> None:
    assert (
        dataset_of_page("/economy/grossdomesticproductgdp/timeseries/abmi/qna") == "QNA"
    )


def test_latest_period_end_uses_three_month_end_and_ignores_blanks() -> None:
    assert latest_period_end(DOCUMENT) == pd.Timestamp("2026-08-31")
    assert latest_period_end({"months": []}) is None
    mid_year = {
        "years": [{"date": "2016", "label": "2016", "value": "1"}],
        "quarters": [{"date": "2016 Q2", "label": "2016 Q2", "value": "1"}],
    }
    assert latest_period_end(mid_year) == pd.Timestamp("2016-06-30")
