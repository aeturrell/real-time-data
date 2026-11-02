"""Tests for the survey sources, using small tables shaped like the real files."""

from datetime import datetime

import pandas as pd
import pytest

from realtime.cache import check_is_workbook
from realtime.http import HttpClient
from realtime.sources.agents import first_report_from, report_date_for_month
from realtime.sources.bics import (
    dependable_release,
    london_date,
    read_answer_table,
    read_window_ends,
    version_dates,
)
from realtime.sources.boe_site import BankPage, parse_published_date, spreadsheet_links
from realtime.sources.credit_conditions import (
    _quarter_columns,
    annex_link,
    normalise_question,
    read_annex_sheet,
)
from realtime.sources.dmp import data_file_link, is_survey_month
from realtime.sources.survey_tables import (
    find_column,
    read_survey_table,
    to_number,
)

BICS_HEADER = ("Dates", "Wave", "Trading Status", "Industry/Size Band", "Yes", "No")
LAST_WAVE = 166


def page(slug: str, *links: str) -> BankPage:
    """Builds a page for tests.

    Args:
        slug: The last part of the page's path.
        *links: The spreadsheets the page links to.

    Returns:
        The page.
    """
    return BankPage(f"/section/2024/{slug}", pd.Timestamp("2024-01-04"), True, links)


def test_published_date_read_from_page_text() -> None:
    html = '<p class="published-date">Published on <time>7 April 2022</time></p>'
    assert parse_published_date(html) == pd.Timestamp("2022-04-07")


def test_page_without_published_date_is_rejected() -> None:
    with pytest.raises(ValueError):
        parse_published_date('<meta name="datePublished" content="2026-06-01">')


def test_spreadsheet_links_are_complete_and_unrepeated() -> None:
    html = '<a href="/-/media/a.xlsx">a</a><a href="/-/media/a.xlsx">b</a>'
    links = spreadsheet_links(html)
    assert links == ["https://www.bankofengland.co.uk/-/media/a.xlsx"]


def test_error_page_is_not_accepted_as_workbook() -> None:
    with pytest.raises(ValueError):
        check_is_workbook(b"<html>Sorry</html>")


def test_bank_of_england_has_a_longer_gap_than_the_default(monkeypatch) -> None:
    client = HttpClient(min_interval_seconds=0.0)
    waits: list[float] = []
    monkeypatch.setattr("realtime.http.time.sleep", waits.append)
    client._wait_for_turn("www.bankofengland.co.uk")
    client._wait_for_turn("www.bankofengland.co.uk")
    assert waits and waits[-1] > 2.5


def test_to_number_treats_markers_as_missing() -> None:
    assert to_number(0.25) == 0.25
    assert to_number("0.25") == 0.25
    assert to_number("[c]") is None
    assert to_number(".") is None
    assert to_number(None) is None


def test_group_headings_carry_right_and_titles_are_ignored() -> None:
    rows = [
        ("Realised employment growth, %",),
        (None, "Realised employment growth", None, "Expected employment growth", None),
        (
            "Survey date",
            "Single month",
            "3 month average",
            "Single month",
            "3 month average",
        ),
        (datetime(2017, 1, 1), 5.1, 4.4, 0.5, 0.0),
        ("Return to Contents", None, None, None, None),
    ]
    table = read_survey_table(rows, is_survey_month)
    assert len(table.rows) == 1
    assert find_column(table.columns, "Single month", ("realised",)) == 1
    assert find_column(table.columns, "Single month", ("expected",)) == 3
    assert find_column(table.columns, "Quarterly average", ()) is None


def test_blank_bottom_heading_is_not_a_column() -> None:
    rows = [
        ("Group one", "Group two", None, None),
        ("Survey date", "Single month", None, "3 month average"),
        (datetime(2017, 1, 1), 1.0, None, 2.0),
    ]
    table = read_survey_table(rows, is_survey_month)
    assert 2 not in table.columns
    assert find_column(table.columns, "3 month average", ()) == 3


def test_ambiguous_column_is_an_error_not_a_guess() -> None:
    columns = {1: "Realised | Single month", 2: "Expected | Single month"}
    with pytest.raises(ValueError):
        find_column(columns, "Single month", ())


def test_answer_must_equal_the_final_heading_level() -> None:
    columns = {1: "High", 2: "Very high"}
    assert find_column(columns, "High", ()) == 1
    assert find_column(columns, "Very high", ()) == 2


@pytest.mark.parametrize(
    ("label", "expected"),
    [
        (datetime(2017, 1, 1), True),
        ("Jan-17", True),
        ("Survey date", False),
        ("Return to Contents", False),
        (None, False),
    ],
)
def test_survey_months_come_as_dates_or_text(label: object, expected: bool) -> None:
    assert is_survey_month(label) is expected


def test_dmp_prefers_own_monthly_file() -> None:
    chosen = data_file_link(
        page(
            "june-2024",
            "https://x/quarterly-dmp-data-june-2024.xlsx",
            "https://x/monthly-dmp-data-june-2024.xlsx",
            "https://x/monthly-dmp-data-may-2024.xlsx",
        )
    )
    assert chosen == "https://x/monthly-dmp-data-june-2024.xlsx"


def test_dmp_uses_quarterly_file_only_when_it_is_the_months_own() -> None:
    december = page(
        "december-2023",
        "https://x/quarterly-dmp-data-december-2023.xlsx",
        "https://x/quarterly-dmp-data-october-2023.xlsx",
    )
    assert data_file_link(december) == "https://x/quarterly-dmp-data-december-2023.xlsx"
    assert data_file_link(page("march-2024", "https://x/monthly-may-2024.xlsx")) is None


def test_ccs_ignores_a_later_quarters_annex() -> None:
    chosen = annex_link(
        page("2018-q2", "https://x/2018-q2-annex.xlsx", "https://x/2019-q4-annex.xlsx")
    )
    assert chosen == "https://x/2018-q2-annex.xlsx"
    assert annex_link(page("2014-q4", "https://x/2019-q4-annex.xlsx")) is None


def test_ccs_footnote_marks_are_removed() -> None:
    assert (
        normalise_question("How have credit card limits changed?(d)*")
        == "How have credit card limits changed?"
    )


def test_ccs_annex_reads_questions_and_quarters() -> None:
    rows = [
        ("Corporate annex", None, None, None, None),
        (None, None, 2007, None, 2008),
        (None, None, "Q3", "Q4", "Q1"),
        ("How has demand changed?(c)", "Past three months", 1.5, "n/a**", 2.5),
        (None, "Next three months", 3.5, 4.5, None),
    ]
    table = read_annex_sheet(rows)
    past = table[("How has demand changed?", "Past three months")]
    assert past == {pd.Period("2007Q3"): 1.5, pd.Period("2008Q1"): 2.5}
    following = table[("How has demand changed?", "Next three months")]
    assert following[pd.Period("2007Q4")] == 4.5


def test_ccs_sheet_without_quarters_is_rejected() -> None:
    with pytest.raises(ValueError):
        read_annex_sheet([("Contents",), ("Secured",)])


def test_agents_month_is_dated_by_first_report_after_the_fifteenth() -> None:
    reports = [pd.Timestamp(day) for day in ("2026-09-11", "2026-10-30", "2026-12-11")]
    retrieved = pd.Timestamp("2026-12-20")
    assert report_date_for_month(pd.Period("2026-10"), reports, retrieved) == reports[1]
    assert report_date_for_month(pd.Period("2026-11"), reports, retrieved) == reports[2]


def test_agents_month_after_every_report_uses_download_date() -> None:
    retrieved = pd.Timestamp("2026-12-20")
    reports = [pd.Timestamp("2026-10-30")]
    assert report_date_for_month(pd.Period("2026-12"), reports, retrieved) == retrieved


def test_agents_column_public_date_is_first_report_in_that_month() -> None:
    reports = [pd.Timestamp("2006-01-19"), pd.Timestamp("2006-02-16")]
    assert (
        first_report_from(pd.Period("2006-02"), reports, pd.Timestamp("2026-11-02"))
        == reports[1]
    )


def test_bics_timestamps_are_read_as_london_dates() -> None:
    assert london_date("2026-10-07T23:00:00.000Z") == pd.Timestamp("2026-10-08")
    assert london_date("2026-01-22T00:00:00.000Z") == pd.Timestamp("2026-01-22")


def test_bics_version_dates_follow_each_replacement() -> None:
    versions = [
        {"uri": "/v1", "updateDate": "2022-05-05T14:42:00.000Z"},
        {"uri": "/v2", "updateDate": "2022-05-19T08:30:00.000Z"},
    ]
    dates = version_dates(pd.Timestamp("2022-05-05"), versions)
    assert dates == [
        pd.Timestamp("2022-05-05"),
        pd.Timestamp("2022-05-05"),
        pd.Timestamp("2022-05-19"),
    ]


def test_bics_survey_window_end_is_read_for_each_wave() -> None:
    rows = [
        ("Dates",),
        ("Wave", "Reference period", "Survey live period"),
        ("Wave 166", "1 Sep 2026 to 30 Sep 2026", "5 October 2026 to 18 October 2026"),
        ("Wave 07", "1 June 2020 to 14 June 2020", "15 June 2020 to 28 June 2020"),
    ]
    assert read_window_ends(rows) == {
        LAST_WAVE: pd.Timestamp("2026-10-18"),
        7: pd.Timestamp("2020-06-28"),
    }


def test_bics_reads_all_businesses_for_the_chosen_group() -> None:
    rows = [
        ("Question",),
        ("As a percentage of businesses",),
        BICS_HEADER,
        ("Sep", "Wave 166", "Currently trading", "Manufacturing", 0.9, 0.1),
        ("Sep", "Wave 166", "Currently trading", "All businesses", 0.2, 0.8),
        ("Sep", "Wave 166", "Temporarily closed", "All businesses", 0.5, 0.5),
        ("Aug", "Wave 165", "Currently trading", "All businesses", "[c]", 0.7),
    ]
    assert read_answer_table(rows, "Yes", "Currently trading") == {LAST_WAVE: 0.2}
    assert read_answer_table(rows, "Maybe", "Currently trading") == {}


def test_bics_sheet_for_a_different_group_gives_nothing() -> None:
    rows = [
        ("Question",),
        ("As a percentage of businesses currently trading",),
        ("Dates", "Wave", "Industry/Size Band", "Yes"),
        ("Sep", "Wave 166", "All businesses", 0.2),
    ]
    assert read_answer_table(rows, "Yes", "Currently trading") == {LAST_WAVE: 0.2}
    assert read_answer_table(rows, "Yes", "Not permanently stopped trading") == {}


def test_bics_sound_release_date_is_kept() -> None:
    releases = [pd.Timestamp("2026-07-02"), pd.Timestamp("2026-08-06")]
    assert dependable_release(
        pd.Timestamp("2026-08-06"), pd.Timestamp("2026-08-02"), releases
    ) == (pd.Timestamp("2026-08-06"), "day")


def test_bics_release_before_survey_closed_uses_next_release() -> None:
    releases = [pd.Timestamp("2026-07-02"), pd.Timestamp("2026-08-06")]
    assert dependable_release(
        pd.Timestamp("2026-07-02"), pd.Timestamp("2026-07-19"), releases
    ) == (pd.Timestamp("2026-08-06"), "upper_bound")


def test_bics_correction_cannot_come_before_the_first_version() -> None:
    versions = [{"uri": "/v1", "updateDate": "2023-02-23T10:48:00.000Z"}]
    dates = version_dates(pd.Timestamp("2023-03-09"), versions)
    assert dates == [pd.Timestamp("2023-02-23"), pd.Timestamp("2023-02-23")]


def test_quarter_columns_count_years_when_a_written_year_is_mistyped() -> None:
    year_row = (None, 2013, None, None, None, 2013, None, None, None, 2015)
    quarter_row = (None, "Q1", "Q2", "Q3", "Q4", "Q1", "Q2", "Q3", "Q4", "Q1")

    columns = _quarter_columns(year_row, quarter_row)

    assert columns[5] == pd.Period("2014Q1", "Q")
    assert columns[9] == pd.Period("2015Q1", "Q")


def test_quarter_columns_ignore_a_misplaced_or_missing_year() -> None:
    year_row = (2015, None, None, None, None, None, 2016, None, 2017)
    quarter_row = ("Q1", "Q2", "Q3", "Q4", "Q1", "Q2", "Q3", "Q4", "Q1")

    columns = _quarter_columns(year_row, quarter_row)

    assert columns[4] == pd.Period("2016Q1", "Q")
    assert columns[8] == pd.Period("2017Q1", "Q")
