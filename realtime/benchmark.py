"""Comparing the archive with the OECD's independent real-time database.

The OECD's short-term economic statistics revisions database keeps every monthly
edition of its Main Economic Indicators since February 1999, each holding the UK
figures as the OECD had them that month. It was assembled separately from this
archive, so agreement with it is independent evidence that our vintages are right.

For each benchmark, growth rates are worked out from the OECD's figures in every
edition (indices are rebased by the OECD, so growth is compared, not levels) and
checked against ours for the most recent periods in that edition, in two ways:

- strict: our figures as they stood the day before the edition's month began;
- nearby: the closest of our vintages from two months before to one month after
  the start of the edition's month, which allows for the OECD collecting figures
  a little before or after that date.

Agreement is reported within rounding (0.05 percentage points) and within a looser
0.15 points. The looser test is needed for OECD indices: they are rebased from the
ONS's one-decimal indices, so growth worked out from them can be about 0.1 points
away from the rate ONS published, even when both come from the same release.

The OECD's consumer prices follow whichever UK measure was the headline at the
time, so each consumer price benchmark covers only the editions in which the OECD
used that measure. Unemployment and retail sales are not compared: the OECD's UK
series are not the same measures as the ONS three-month unemployment rate or the
headline retail sales volume index.
"""

import io
from dataclasses import dataclass

import pandas as pd
from loguru import logger

from realtime.cache import DownloadCache
from realtime.http import DownloadError
from realtime.store import revision_triangle

OECD_REVISIONS_URL = (
    "https://sdmx.oecd.org/public/rest/data/"
    "OECD.SDD.STES,DSD_STES_REVISIONS@DF_STES_REVISIONS,4.0/"
    "GBR.{frequency}.{measure}..{activity}.?format=csvfile&startPeriod={start}"
)
OECD_CSV_HEADER = b"DATAFLOW,"
OECD_START_YEAR = 1990
PANDAS_FREQUENCY = {"M": "M", "Q": "Q-DEC"}
PERCENT = 100
RECENT_PERIODS = 12
DAYS_BEFORE_EDITION = 62
DAYS_AFTER_EDITION = 31
ONE_DAY = pd.Timedelta(days=1)
ROUNDING_TOLERANCE = 0.05 + 1e-9
LOOSE_TOLERANCE = 0.15 + 1e-9


@dataclass(frozen=True)
class Benchmark:
    """One comparison between an OECD series and one of ours.

    Attributes:
        name: A short description.
        measure: The OECD measure code, for example "B1GQ_Q".
        frequency: "M" or "Q".
        series_id: Our series.
        ours_is_rate: True if our series is already a growth rate; False if it
            is a level from which growth is worked out.
        lag: The number of periods growth is measured over (1 for quarter on
            quarter or month on month, 4 or 12 for a year on year).
        activity: The OECD activity code, if one must be picked.
        editions_from: The first OECD edition to compare ("YYYY-MM"), if limited.
        editions_to: The last OECD edition to compare ("YYYY-MM"), if limited.
    """

    name: str
    measure: str
    frequency: str
    series_id: str
    ours_is_rate: bool
    lag: int
    activity: str = ""
    editions_from: str | None = None
    editions_to: str | None = None

    def covers(self, edition: pd.Timestamp) -> bool:
        """Checks whether an OECD edition falls in the range this benchmark uses.

        Args:
            edition: The first day of the edition's month.

        Returns:
            True if the edition is within the range.
        """
        month = pd.Period(edition, "M")
        after_start = self.editions_from is None or month >= pd.Period(
            self.editions_from, "M"
        )
        before_end = self.editions_to is None or month <= pd.Period(
            self.editions_to, "M"
        )
        return after_start and before_end


BENCHMARKS = (
    Benchmark("GDP volume, q/q", "B1GQ_Q", "Q", "ABMI", False, 1),
    Benchmark("GDP volume, q/q (triangle)", "B1GQ_Q", "Q", "IHYQ", True, 1),
    Benchmark("GDP current prices, q/q", "B1GQ_V", "Q", "YBHA", False, 1),
    Benchmark("Government consumption, q/q", "P3_S13_Q", "Q", "NMRY", False, 1),
    Benchmark("Gross fixed capital formation, q/q", "P51G_Q", "Q", "NPQT", False, 1),
    Benchmark("Exports, q/q", "P6_Q", "Q", "IKBK", False, 1),
    Benchmark("Imports, q/q", "P7_Q", "Q", "IKBL", False, 1),
    Benchmark("GDP deflator, y/y (triangle)", "B1GQ_D", "Q", "YBGB_YOY", True, 4),
    Benchmark("Production, m/m (triangle)", "PRVM", "M", "IOP_MOM", True, 1, "BTE"),
    Benchmark("Production, m/m", "PRVM", "M", "K222", False, 1, "BTE"),
    Benchmark("RPI, y/y", "CP", "M", "CHAW", False, 12, editions_to="2006-12"),
    Benchmark(
        "CPI, y/y",
        "CP",
        "M",
        "D7G7",
        True,
        12,
        editions_from="2008-01",
        editions_to="2017-12",
    ),
    Benchmark("CPIH, y/y", "CP", "M", "L55O", True, 12, editions_from="2019-01"),
)


def benchmark_report(
    archive: pd.DataFrame,
    cache: DownloadCache,
    benchmarks: tuple[Benchmark, ...] = BENCHMARKS,
) -> pd.DataFrame:
    """Compares the archive with the OECD revisions database.

    Args:
        archive: The archive, as returned by read_archive.
        cache: The download cache (OECD files are cached like any other).
        benchmarks: The comparisons to make.

    Returns:
        One row per benchmark with the number of values compared and the share
        agreeing under the strict and nearby tests.
    """
    rows = []
    for benchmark in benchmarks:
        try:
            oecd = oecd_editions(cache, benchmark)
        except (DownloadError, ValueError) as error:
            logger.error(f"Skipping benchmark {benchmark.name}: {error}")
            continue
        rows.append(compare(archive, oecd, benchmark))
    return pd.DataFrame(rows)


def oecd_editions(cache: DownloadCache, benchmark: Benchmark) -> pd.DataFrame:
    """Downloads one OECD series in every edition.

    Args:
        cache: The download cache.
        benchmark: The comparison whose OECD series is wanted.

    Returns:
        A table with one row per edition (indexed by the first day of the
        edition's month) and one column per period, holding the OECD's figures.
    """
    url = OECD_REVISIONS_URL.format(
        frequency=benchmark.frequency,
        measure=benchmark.measure,
        activity=benchmark.activity,
        start=OECD_START_YEAR,
    )
    cached = cache.fetch(url, immutable=False, validate=_check_is_oecd_csv)
    raw = pd.read_csv(io.BytesIO(cached.content))
    table = raw.pivot_table(
        index="EDITION", columns="TIME_PERIOD", values="OBS_VALUE", aggfunc="last"
    )
    table.index = pd.DatetimeIndex(
        [pd.Period(str(edition), "M").start_time for edition in table.index]
    )
    table.columns = pd.PeriodIndex(
        [period.replace("-Q", "Q") for period in table.columns],
        freq=PANDAS_FREQUENCY[benchmark.frequency],
    )
    return _complete_columns(table)


def compare(
    archive: pd.DataFrame, oecd: pd.DataFrame, benchmark: Benchmark
) -> dict[str, object]:
    """Checks the OECD's growth rates in every edition against ours.

    Args:
        archive: The archive, as returned by read_archive.
        oecd: The OECD figures from oecd_editions.
        benchmark: The comparison.

    Returns:
        The comparison summary.
    """
    ours = _our_growth(archive, benchmark)
    theirs = _growth(oecd, benchmark.lag)
    results = [
        result
        for edition in theirs.index
        if benchmark.covers(edition) and edition - ONE_DAY >= ours.index.min()
        for result in _compare_edition(ours, theirs.loc[edition], edition, benchmark)
    ]
    summary: dict[str, object] = {
        "benchmark": benchmark.name,
        "oecd": benchmark.measure,
        "ours": benchmark.series_id,
    }
    if not results:
        return summary | {"compared": 0}
    checked = pd.DataFrame(results)
    return summary | {
        "compared": len(checked),
        "editions": checked["edition"].nunique(),
        "first_edition": f"{checked['edition'].min():%Y-%m}",
        "strict_within_0.05": _share_within(checked["strict"], ROUNDING_TOLERANCE),
        "nearby_within_0.05": _share_within(checked["nearby"], ROUNDING_TOLERANCE),
        "nearby_within_0.15": _share_within(checked["nearby"], LOOSE_TOLERANCE),
    }


def _share_within(differences: pd.Series, tolerance: float) -> float:
    """Gives the share of differences no larger than a tolerance.

    Args:
        differences: Absolute differences in percentage points.
        tolerance: The tolerance.

    Returns:
        The share, to three decimal places.
    """
    return round((differences <= tolerance).mean(), 3)


def _compare_edition(
    ours: pd.DataFrame, theirs: pd.Series, edition: pd.Timestamp, benchmark: Benchmark
) -> list[dict[str, object]]:
    """Checks the most recent periods of one OECD edition against our vintages.

    Args:
        ours: Our growth rates, one row per vintage date.
        theirs: The OECD's growth rates in this edition.
        edition: The first day of the edition's month.
        benchmark: The comparison.

    Returns:
        One result per period checked, holding the absolute difference from our
        figures at the strict date and the smallest difference from our nearby
        vintages.
    """
    published = theirs.dropna()
    if published.empty:
        return []
    strict = ours.loc[: edition - ONE_DAY].iloc[-1]
    nearby = _nearby_vintages(ours, edition)
    latest = published.index.max()
    periods = [latest - offset for offset in range(RECENT_PERIODS)]
    return [
        {
            "edition": edition,
            "period": period,
            "strict": abs(strict.get(period) - published[period]),
            "nearby": (nearby[period] - published[period]).abs().min(),
        }
        for period in periods
        if period in published.index
        and period in nearby.columns
        and pd.notna(strict.get(period))
    ]


def _nearby_vintages(ours: pd.DataFrame, edition: pd.Timestamp) -> pd.DataFrame:
    """Picks our vintages around the start of an edition's month.

    Args:
        ours: Our growth rates, one row per vintage date.
        edition: The first day of the edition's month.

    Returns:
        The vintage in force at the start of the window and every vintage in it.
    """
    window_start = edition - pd.Timedelta(days=DAYS_BEFORE_EDITION)
    window_end = edition + pd.Timedelta(days=DAYS_AFTER_EDITION)
    in_force = ours.loc[:window_start].tail(1)
    within = ours.loc[(ours.index > window_start) & (ours.index <= window_end)]
    return pd.concat([in_force, within])


def _our_growth(archive: pd.DataFrame, benchmark: Benchmark) -> pd.DataFrame:
    """Gives our growth rates for a benchmark, one row per vintage date.

    Args:
        archive: The archive, as returned by read_archive.
        benchmark: The comparison.

    Returns:
        Growth rates by vintage date and reference period.
    """
    rows = archive.loc[archive["frequency"] == benchmark.frequency]
    triangle = revision_triangle(rows, benchmark.series_id)
    triangle.columns = pd.PeriodIndex(
        list(triangle.columns), freq=PANDAS_FREQUENCY[benchmark.frequency]
    )
    table = _complete_columns(triangle)
    return table if benchmark.ours_is_rate else _growth(table, benchmark.lag)


def _complete_columns(table: pd.DataFrame) -> pd.DataFrame:
    """Sorts a table's period columns and fills in any missing periods.

    Args:
        table: A table with pd.Period columns.

    Returns:
        The table with one column for every period from the first to the last.
    """
    periods = pd.period_range(
        table.columns.min(), table.columns.max(), freq=table.columns.freq
    )
    return table.reindex(columns=periods)


def _growth(levels: pd.DataFrame, lag: int) -> pd.DataFrame:
    """Works out percentage growth along each row of a table of levels.

    Args:
        levels: Levels with consecutive period columns.
        lag: The number of periods growth is measured over.

    Returns:
        Percentage growth.
    """
    return (levels / levels.shift(lag, axis=1) - 1) * PERCENT


def _check_is_oecd_csv(content: bytes) -> None:
    """Checks that a download is an OECD SDMX CSV file.

    Args:
        content: The downloaded bytes.

    Raises:
        ValueError: If the content is not an OECD CSV file.
    """
    if not content.startswith(OECD_CSV_HEADER):
        raise ValueError("not an OECD SDMX CSV file")
