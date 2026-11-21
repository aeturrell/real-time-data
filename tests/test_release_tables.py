import io
import zipfile
from datetime import datetime

import numpy as np
import pandas as pd

from realtime.sources.release_tables import (
    read_label,
    read_workbooks,
    series_in_sheet,
    table_title,
)

NAN = np.nan
MONTHLY = {"ABCD": "period"}
ROLLING = {"MGSX": "3m_ending"}


def test_reads_year_and_month_split_across_columns_with_annual_rows() -> None:
    sheet = pd.DataFrame(
        [
            ["Index of Production", NAN, NAN, NAN],
            [NAN, NAN, NAN, "ABCD"],
            [NAN, 2011, NAN, 100.0],
            [NAN, 2012, NAN, 101.0],
            [NAN, 2012, "Nov", 101.5],
            [NAN, NAN, "Dec", 102.0],
            [NAN, NAN, "Jan", 103.0],
        ]
    )
    figures = series_in_sheet(sheet, MONTHLY)
    assert figures["reference_period"].tolist() == [
        "2011",
        "2012",
        "2012-11",
        "2012-12",
        "2013-01",
    ]
    assert figures["frequency"].tolist() == ["A", "A", "M", "M", "M"]


def test_skips_a_block_of_changes_headed_by_the_same_code() -> None:
    sheet = pd.DataFrame(
        [
            [NAN, NAN, "ABCD"],
            [NAN, NAN, NAN],
            ["Percentage change, latest month on previous month", NAN, NAN],
            ["2013", "Apr", 0.5],
            [NAN, "May", -0.2],
        ]
    )
    assert series_in_sheet(sheet, MONTHLY).empty


def test_passes_over_a_levels_heading_but_stops_at_change_rows() -> None:
    sheet = pd.DataFrame(
        [
            [NAN, "ABCD", "EFGH"],
            ["Levels (thousands)", NAN, NAN],
            ["2013 Jan", 10.0, 1.0],
            ["2013 Feb", 11.0, 2.0],
            ["Change on year", 3.0, 1.0],
            ["2013 Mar", 99.0, 99.0],
        ]
    )
    figures = series_in_sheet(sheet, MONTHLY)
    assert figures["value"].tolist() == [10.0, 11.0]


def test_stops_at_the_next_row_of_codes() -> None:
    sheet = pd.DataFrame(
        [
            [NAN, "ABCD", "EFGH"],
            ["2013 Jan", 10.0, 1.0],
            [NAN, "WXYZ", "ABCD"],
            ["2013 Feb", 500.0, 11.0],
        ]
    )
    figures = series_in_sheet(sheet, MONTHLY)
    assert figures["value"].tolist() == [10.0, 11.0]
    assert figures["reference_period"].tolist() == ["2013-01", "2013-02"]


def test_three_month_windows_are_kept_only_for_rolling_series() -> None:
    sheet = pd.DataFrame(
        [
            [NAN, "MGSX"],
            ["Oct-Dec 2001", 5.1],
            ["Nov- Jan 2002", 5.2],
            ["2002 Feb", 5.3],
        ]
    )
    rolling = series_in_sheet(sheet, ROLLING)
    assert rolling["reference_period"].tolist() == ["2001-12", "2002-01"]
    assert set(rolling["reference_period_basis"]) == {"3m_ending"}
    assert series_in_sheet(sheet.replace("MGSX", "ABCD"), MONTHLY)[
        "reference_period"
    ].tolist() == ["2002-02"]


def test_dates_three_months_apart_are_quarters() -> None:
    sheet = pd.DataFrame(
        [
            [NAN, "ABCD"],
            [datetime(2012, 3, 1), 1.0],
            [datetime(2012, 6, 1), 2.0],
            [datetime(2012, 9, 1), 3.0],
        ]
    )
    figures = series_in_sheet(sheet, MONTHLY)
    assert figures["reference_period"].tolist() == ["2012Q1", "2012Q2", "2012Q3"]
    assert set(figures["frequency"]) == {"Q"}


def test_labels_without_spaces_and_with_markers_are_read() -> None:
    label = read_label(np.array([NAN, "2006Oct (r)"], dtype=object))
    assert (label.year, label.month, label.words) == (2006, 10, "")
    marked = read_label(np.array(["June #"], dtype=object))
    assert (marked.month, marked.words) == (6, "")
    stopped = read_label(np.array([NAN, "2011 Jun", "(r.)"], dtype=object))
    assert (stopped.year, stopped.month, stopped.words) == (2011, 6, "")


def test_a_footnote_naming_a_month_ends_the_table() -> None:
    sheet = pd.DataFrame(
        [
            [NAN, "ABCD"],
            ["2013 Jan", 10.0],
            ["1. From July 2009 a bank is counted in the private sector", NAN],
            ["2013 Feb", 99.0],
        ]
    )
    assert series_in_sheet(sheet, MONTHLY)["value"].tolist() == [10.0]


def test_table_title_is_the_first_text_at_the_top() -> None:
    sheet = pd.DataFrame([[NAN, "  CLA01   Claimant count "], [NAN, NAN]])
    assert table_title(sheet) == "CLA01 Claimant count"


def test_two_digit_years_before_1950_are_this_century() -> None:
    sheet = pd.DataFrame(
        [
            [NAN, "ABCD"],
            ["Mar 59", 1.0],
            ["Jun 59", 2.0],
            ["Mar 11", 3.0],
            ["Jun 11", 4.0],
        ]
    )
    figures = series_in_sheet(sheet, MONTHLY)
    assert figures["reference_period"].tolist() == [
        "1959Q1",
        "1959Q2",
        "2011Q1",
        "2011Q2",
    ]


def test_a_dash_is_zero_and_two_dots_are_missing() -> None:
    sheet = pd.DataFrame(
        [[NAN, "ABCD"], ["2015 Jan", 0.3], ["2015 Feb", "-"], ["2015 Mar", ".."]]
    )
    figures = series_in_sheet(sheet, MONTHLY)
    assert figures["value"].tolist() == [0.3, 0.0]


def test_figures_and_symbols_among_the_labels_are_not_words() -> None:
    sheet = pd.DataFrame(
        [
            [NAN, NAN, NAN, "ABCD"],
            [2010, "Q3", "1.2", 0.4],
            [NAN, "Q4", "-", -0.2],
            [2011, "Q1", "..", 0.5],
        ]
    )
    figures = series_in_sheet(sheet, MONTHLY)
    assert figures["reference_period"].tolist() == ["2010Q3", "2010Q4", "2011Q1"]


def test_a_whole_number_figure_after_the_label_is_not_its_year() -> None:
    sheet = pd.DataFrame(
        [
            [NAN, NAN, NAN, "ABCD"],
            ["1988 Oct", 2120.0, "-", 2120.0],
            ["1988 Nov", 2069.0, "-", 2069.0],
        ]
    )
    figures = series_in_sheet(sheet, MONTHLY)
    assert figures["reference_period"].tolist() == ["1988-10", "1988-11"]


def test_a_zip_file_saved_under_a_spreadsheet_name_is_opened() -> None:
    buffer = io.BytesIO()
    pd.DataFrame([[NAN, "ABCD"], ["2013 Jan", 1.0]]).to_excel(
        buffer, header=False, index=False
    )
    spreadsheet = buffer.getvalue()
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w") as zipped:
        zipped.writestr("Revisions.xlsx", spreadsheet)
    assert [name for name, _ in read_workbooks(spreadsheet, "a.xlsx")] == ["a.xlsx"]
    assert [name for name, _ in read_workbooks(archive.getvalue(), "b.xls")] == [
        "b.xls:Revisions.xlsx"
    ]


def test_a_repeated_heading_carries_on_into_quarterly_figures() -> None:
    sheet = pd.DataFrame(
        [
            [NAN, "ABCD"],
            ["Seasonally adjusted", NAN],
            ["2011", 6.7],
            ["2012", 6.8],
            ["Seasonally adjusted", NAN],
            ["2012 Q1", 7.0],
            ["2012 Q2", 6.6],
            ["Not seasonally adjusted", NAN],
            ["2012 Q1", 9.9],
        ]
    )
    figures = series_in_sheet(sheet, MONTHLY)
    assert figures["reference_period"].tolist() == ["2011", "2012", "2012Q1", "2012Q2"]
    assert figures["value"].tolist() == [6.7, 6.8, 7.0, 6.6]
