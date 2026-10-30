import pandas as pd

from realtime.release_dates import PageVersion, assign_vintages, london_date


def test_london_date_handles_british_summer_time() -> None:
    assert london_date("2015-09-29T23:00:00.000Z") == pd.Timestamp("2015-09-30")
    assert london_date("2026-01-15T07:00:00.000Z") == pd.Timestamp("2026-01-15")
    assert london_date(None) is None


def test_post_launch_versions_use_the_time_the_previous_version_was_replaced() -> None:
    versions = [
        PageVersion("v1", pd.Timestamp("2016-03-31"), pd.Timestamp("2016-06-30")),
        PageVersion("v2", pd.Timestamp("2016-06-30"), pd.Timestamp("2016-09-30")),
        PageVersion("current", pd.Timestamp("2016-09-30"), None),
    ]
    vintages = assign_vintages(versions)
    assert [vintage.vintage_date for vintage in vintages] == [
        pd.Timestamp("2016-03-31"),
        pd.Timestamp("2016-06-30"),
        pd.Timestamp("2016-09-30"),
    ]
    assert {vintage.precision for vintage in vintages} == {"day"}


def test_stale_pre_launch_release_date_becomes_an_upper_bound() -> None:
    versions = [
        PageVersion("v1", pd.Timestamp("2015-09-30"), pd.Timestamp("2015-09-30")),
        PageVersion("v2", pd.Timestamp("2015-09-30"), pd.Timestamp("2016-03-31")),
        PageVersion("current", pd.Timestamp("2016-03-31"), None),
    ]
    vintages = assign_vintages(versions)
    assert vintages[0].vintage_date == pd.Timestamp("2015-09-30")
    assert vintages[1].vintage_date == pd.Timestamp("2016-02-25")
    assert vintages[1].precision == "upper_bound"
    assert vintages[2].vintage_date == pd.Timestamp("2016-03-31")
    assert vintages[2].precision == "day"


def test_same_day_correction_shares_the_vintage_date() -> None:
    versions = [
        PageVersion("v1", pd.Timestamp("2017-01-18"), pd.Timestamp("2017-02-15")),
        PageVersion("v2", pd.Timestamp("2017-02-15"), pd.Timestamp("2017-02-15")),
        PageVersion("current", pd.Timestamp("2017-02-15"), None),
    ]
    vintages = assign_vintages(versions)
    assert (
        vintages[1].vintage_date
        == vintages[2].vintage_date
        == pd.Timestamp("2017-02-15")
    )
    assert vintages[2].precision == "day"


def test_version_dated_before_its_latest_period_ends_is_moved_later() -> None:
    versions = [
        PageVersion("v22", pd.Timestamp("2020-12-22"), pd.Timestamp("2020-12-22")),
        PageVersion(
            "v23",
            pd.Timestamp("2021-03-31"),
            pd.Timestamp("2021-06-30"),
            pd.Timestamp("2020-12-31"),
        ),
        PageVersion("current", pd.Timestamp("2021-06-30"), None),
    ]
    vintages = assign_vintages(versions)
    assert vintages[1].vintage_date == pd.Timestamp("2021-03-31")
    assert vintages[1].precision == "upper_bound"
    assert vintages[2].vintage_date == pd.Timestamp("2021-06-30")


def test_release_date_does_not_move_a_version_that_is_possible() -> None:
    versions = [
        PageVersion("v1", pd.Timestamp("2016-03-16"), pd.Timestamp("2016-06-15")),
        PageVersion(
            "v2",
            pd.Timestamp("2016-08-17"),
            pd.Timestamp("2016-09-14"),
            pd.Timestamp("2016-03-31"),
        ),
        PageVersion("current", pd.Timestamp("2016-09-14"), None),
    ]
    assert assign_vintages(versions)[1].vintage_date == pd.Timestamp("2016-06-15")
