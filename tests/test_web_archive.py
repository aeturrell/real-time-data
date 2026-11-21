import hashlib
import io
import json
from collections.abc import Callable

import numpy as np
import pandas as pd
import pytest

from realtime.cache import CachedFile
from realtime.config import SeriesSpec, WebArchiveSpec, WebArchiveTable, load_config
from realtime.http import DownloadError
from realtime.sources.web_archive import (
    SavedCopy,
    WebArchiveSource,
    choose_tables,
    drop_overtaken_editions,
    edition_names,
    index_url,
    issued_date,
    names_another_edition,
    parse_listing,
)

NAN = np.nan
RELEASE = "abc/example-release"
OLD_SITE = "http://www.ons.gov.uk/ons/rel"
ARCHIVE = "https://webarchive.nationalarchives.gov.uk/ukgwa"
SPEC = WebArchiveSpec(
    release=RELEASE,
    dataset_id="EXA",
    editions="month",
    first_year=2013,
    last_year=2013,
    tables=(
        WebArchiveTable(files=r"^table-1\.xlsx$", series=("ABCD",), scale=0.001),
        WebArchiveTable(files=r"^table-2\.xlsx$", series=("ABCD",), scale=0.001),
    ),
)
SERIES = (
    SeriesSpec(
        series_id="ABCD",
        short_name="example",
        measure="Thousands",
        primary_dataset="EXA",
    ),
)
RELEASE_PAGE = (
    b'<html><meta name="DCTERMS.Issued" content="2013-07-17T09:30:00Z" /></html>'
)


class FakeCache:
    """A download cache that serves fixed content and records what was asked for."""

    def __init__(
        self, content: dict[str, bytes], cached: set[str] | None = None
    ) -> None:
        self._content = content
        self._cached = cached or set()
        self.requested: list[str] = []

    def is_cached(self, url: str) -> bool:
        return url in self._cached

    def fetch(
        self,
        url: str,
        immutable: bool,
        validate: Callable[[bytes], None] | None = None,
    ) -> CachedFile:
        self.requested.append(url)
        if url not in self._content:
            raise DownloadError(f"unavailable: {url}")
        content = self._content[url]
        if validate:
            try:
                validate(content)
            except ValueError as error:
                raise DownloadError(str(error)) from error
        self._cached.add(url)
        return CachedFile(
            url=url,
            content=content,
            sha256=hashlib.sha256(content).hexdigest(),
            retrieved_at=pd.Timestamp("2026-01-01", tz="UTC"),
        )


def workbook(rows: list[list[object]]) -> bytes:
    buffer = io.BytesIO()
    pd.DataFrame(rows).to_excel(buffer, header=False, index=False)
    return buffer.getvalue()


def listing(*copies: tuple[str, str, str]) -> bytes:
    lines = [
        json.dumps(
            {
                "url": f"{OLD_SITE}/{RELEASE}/june-2013/{name}",
                "timestamp": stamp,
                "digest": digest,
            }
        )
        for name, stamp, digest in copies
    ]
    return "\n".join(lines).encode()


def replay(name: str, stamp: str) -> str:
    return f"{ARCHIVE}/{stamp}id_/{OLD_SITE}/{RELEASE}/june-2013/{name}"


def test_edition_names_cover_every_month_or_quarter() -> None:
    assert edition_names(SPEC)[:2] == ["january-2013", "february-2013"]
    quarterly = WebArchiveSpec(**{**SPEC.__dict__, "editions": "quarter"})
    assert edition_names(quarterly) == ["q1-2013", "q2-2013", "q3-2013", "q4-2013"]


def test_index_url_is_fixed_and_bounded_by_the_cutoff() -> None:
    url = index_url(SPEC, "june-2013")
    assert url == (
        "https://webarchive.nationalarchives.gov.uk/ukgwa/cdx?"
        "url=www.ons.gov.uk/ons/rel/abc/example-release/june-2013/*"
        "&filter=status:200&fl=url,timestamp,digest&output=json&to=20161231235959"
    )


def test_listing_leaves_out_other_renderings() -> None:
    content = listing(
        ("table-1.xlsx", "20130801000000", "A"),
        ("table-1.xlsx?format=contrast", "20130801000001", "B"),
    )
    assert [copy.digest for copy in parse_listing(content)] == ["A"]
    assert parse_listing(b"") == []


def test_tables_are_chosen_in_pattern_order_once_each() -> None:
    copies = [
        SavedCopy("http://x/table-2.xlsx", "1", "A"),
        SavedCopy("http://x/table-1.xlsx", "2", "B"),
        SavedCopy("http://x/TABLE-1.xlsx", "3", "B"),
        SavedCopy("http://x/chart.xlsx", "4", "C"),
    ]
    chosen = choose_tables(copies, (r"^table-1", r"^table-\d"))
    assert [[copy.timestamp for copy in group] for group in chosen] == [
        ["2", "3"],
        ["1"],
    ]


def test_issued_date_is_read_from_the_release_page() -> None:
    assert issued_date(RELEASE_PAGE) == pd.Timestamp("2013-07-17")
    with pytest.raises(ValueError):
        issued_date(b"<html></html>")


def test_reads_the_earliest_version_and_dates_it_from_the_release_page() -> None:
    first = workbook([[NAN, "ABCD"], ["2013 Apr", 1000.0], ["2013 May", 2000.0]])
    corrected = workbook([[NAN, "ABCD"], ["2013 Apr", 9000.0], ["2013 May", 9000.0]])
    second = workbook([[NAN, "ABCD"], ["2013 Apr", 1000.0], ["2013 Jun", 3000.0]])
    content = {
        index_url(SPEC, "june-2013"): listing(
            ("index.html", "20130720000000", "P"),
            ("table-1.xlsx", "20130718000000", "A"),
            ("table-1.xlsx", "20130719000000", "A"),
            ("table-1.xlsx", "20130901000000", "C"),
            ("table-2.xlsx", "20130718000000", "D"),
        ),
        replay("index.html", "20130720000000"): RELEASE_PAGE,
        replay("table-1.xlsx", "20130719000000"): first,
        replay("table-1.xlsx", "20130901000000"): corrected,
        replay("table-2.xlsx", "20130718000000"): second,
    }
    cache = FakeCache(content)
    source = WebArchiveSource((SPEC,), SERIES, cache)
    figures = source._edition(SPEC, "june-2013")
    assert figures["reference_period"].tolist() == ["2013-04", "2013-05", "2013-06"]
    assert figures["value"].tolist() == [1.0, 2.0, 3.0]
    assert set(figures["vintage_date"]) == {pd.Timestamp("2013-07-17")}
    assert set(figures["vintage_date_precision"]) == {"day"}
    assert replay("table-1.xlsx", "20130901000000") not in cache.requested


def test_a_cached_copy_is_preferred_to_an_earlier_one() -> None:
    content = {
        replay("table-1.xlsx", "20130718000000"): b"PK first",
        replay("table-1.xlsx", "20130719000000"): b"PK first",
    }
    cache = FakeCache(content, cached={replay("table-1.xlsx", "20130719000000")})
    source = WebArchiveSource((SPEC,), SERIES, cache)
    copies = [
        SavedCopy(f"{OLD_SITE}/{RELEASE}/june-2013/table-1.xlsx", stamp, "A")
        for stamp in ("20130718000000", "20130719000000")
    ]
    chosen = source._first_version(copies, lambda content: None)
    assert chosen.url == replay("table-1.xlsx", "20130719000000")


def test_without_a_release_page_the_date_is_an_upper_bound() -> None:
    content = {
        index_url(SPEC, "june-2013"): listing(("table-1.xlsx", "20130718120000", "A")),
        replay("table-1.xlsx", "20130718120000"): workbook(
            [[NAN, "ABCD"], ["2013 May", 1000.0]]
        ),
    }
    figures = WebArchiveSource((SPEC,), SERIES, FakeCache(content))._edition(
        SPEC, "june-2013"
    )
    assert set(figures["vintage_date"]) == {pd.Timestamp("2013-07-18")}
    assert set(figures["vintage_date_precision"]) == {"upper_bound"}


def test_an_edition_dated_before_its_figures_end_is_skipped() -> None:
    page = b'<html><meta name="DCTERMS.Issued" content="2013-05-10T09:30:00Z" /></html>'
    content = {
        index_url(SPEC, "june-2013"): listing(
            ("index.html", "20130720000000", "P"),
            ("table-1.xlsx", "20130718000000", "A"),
        ),
        replay("index.html", "20130720000000"): page,
        replay("table-1.xlsx", "20130718000000"): workbook(
            [[NAN, "ABCD"], ["2013 May", 1000.0]]
        ),
    }
    figures = WebArchiveSource((SPEC,), SERIES, FakeCache(content))._edition(
        SPEC, "june-2013"
    )
    assert figures.empty


def test_config_rejects_a_web_archive_series_without_a_series_entry(tmp_path) -> None:
    path = tmp_path / "series.toml"
    path.write_text(
        """
[[web_archive]]
release = "abc/example"
dataset = "EXA"
editions = "month"
first_year = 2013
last_year = 2013
tables = [{ files = '^table', series = ["ABCD"] }]
"""
    )
    with pytest.raises(ValueError, match="ABCD"):
        load_config(path)


def test_files_named_for_another_edition_are_recognised() -> None:
    assert names_another_edition("rft-iop5-nov-2011.xls", "december-2011")
    assert not names_another_edition("rft-iop5-dec-2011.xls", "december-2011")
    assert not names_another_edition("chd-iopiom-timeseries-june-15.xls", "june-2015")
    assert not names_another_edition("table-cla01.xls", "june-2013")
    assert names_another_edition("rft-gdp-q1-2013.xls", "q2-2013")
    assert not names_another_edition("rft-gdp-q2-13.xls", "q2-2013")


def test_the_bulletin_page_dates_an_edition_whose_release_page_is_unavailable() -> None:
    content = {
        index_url(SPEC, "june-2013"): listing(
            ("index.html", "20130720000000", "P"),
            ("stb-example-june-2013.html", "20130721000000", "S"),
            ("table-1.xlsx", "20130718000000", "A"),
        ),
        replay("stb-example-june-2013.html", "20130721000000"): RELEASE_PAGE,
        replay("table-1.xlsx", "20130718000000"): workbook(
            [[NAN, "ABCD"], ["2013 May", 1000.0]]
        ),
    }
    figures = WebArchiveSource((SPEC,), SERIES, FakeCache(content))._edition(
        SPEC, "june-2013"
    )
    assert set(figures["vintage_date"]) == {pd.Timestamp("2013-07-17")}
    assert set(figures["vintage_date_precision"]) == {"day"}


def test_only_sheets_with_a_wanted_title_are_read() -> None:
    spec = WebArchiveSpec(
        **{
            **SPEC.__dict__,
            "tables": (
                WebArchiveTable(
                    files=r"^table-\d\.xlsx$",
                    series=("ABCD",),
                    title="^IOP5$",
                    scale=0.001,
                ),
            ),
        }
    )
    content = {
        index_url(spec, "june-2013"): listing(
            ("index.html", "20130720000000", "P"),
            ("table-1.xlsx", "20130718000000", "A"),
            ("table-2.xlsx", "20130718000000", "B"),
        ),
        replay("index.html", "20130720000000"): RELEASE_PAGE,
        replay("table-1.xlsx", "20130718000000"): workbook(
            [["IOP5R", NAN], [NAN, "ABCD"], ["2013 May", -100.0]]
        ),
        replay("table-2.xlsx", "20130718000000"): workbook(
            [["IOP5", NAN], [NAN, "ABCD"], ["2013 May", 101000.0]]
        ),
    }
    figures = WebArchiveSource((spec,), SERIES, FakeCache(content))._edition(
        spec, "june-2013"
    )
    assert figures["value"].tolist() == [101.0]
    assert figures["title"].tolist() == ["IOP5 (table-2.xlsx)"]


def test_each_table_supplies_only_its_own_series_in_its_own_units() -> None:
    spec = WebArchiveSpec(
        **{
            **SPEC.__dict__,
            "tables": (
                WebArchiveTable(
                    files=r"^table-1\.xlsx$", series=("ABCD",), scale=0.001
                ),
                WebArchiveTable(files=r"^table-2\.xlsx$", series=("ABCD",)),
            ),
        }
    )
    content = {
        index_url(spec, "june-2013"): listing(
            ("index.html", "20130720000000", "P"),
            ("table-1.xlsx", "20130718000000", "A"),
            ("table-2.xlsx", "20130718000000", "B"),
        ),
        replay("index.html", "20130720000000"): RELEASE_PAGE,
        replay("table-1.xlsx", "20130718000000"): workbook(
            [[NAN, "ABCD", "EFGH"], ["2013 Apr", 1500.0, 7.0]]
        ),
        replay("table-2.xlsx", "20130718000000"): workbook(
            [[NAN, "ABCD"], ["2013 Apr", 1.5], ["2013 May", 1.6]]
        ),
    }
    figures = WebArchiveSource((spec,), SERIES, FakeCache(content))._edition(
        spec, "june-2013"
    )
    assert figures["series_id"].tolist() == ["ABCD", "ABCD"]
    assert figures["value"].tolist() == [1.5, 1.6]


def test_an_upper_bound_edition_dated_after_a_later_edition_is_dropped() -> None:
    def edition(day: str, precision: str) -> pd.DataFrame:
        return pd.DataFrame(
            {"vintage_date": [pd.Timestamp(day)], "vintage_date_precision": [precision]}
        )

    editions = [
        edition("2013-03-20", "day"),
        edition("2013-04-25", "upper_bound"),
        edition("2013-04-17", "day"),
        edition("2013-05-01", "upper_bound"),
    ]
    kept = drop_overtaken_editions(editions, RELEASE)
    assert [frame["vintage_date"].iloc[0] for frame in kept] == [
        pd.Timestamp("2013-03-20"),
        pd.Timestamp("2013-04-17"),
        pd.Timestamp("2013-05-01"),
    ]
