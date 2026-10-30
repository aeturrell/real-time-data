"""Change-only storage of vintages, and tools for working with the stored archive.

The archive keeps a row only when a value is first published or later revised.
Any past vintage can be rebuilt with as_of, and any revisions triangle with
revision_triangle.
"""

from pathlib import Path

import pandas as pd
from loguru import logger

from realtime.periods import FREQUENCY_TO_PANDAS, PANDAS_TO_FREQUENCY, string_to_period
from realtime.schema import KEY_COLUMNS, conform, validate
from realtime.settings import ARCHIVE_PATH

VALUE_COMPARISON_DECIMALS = 9
RANK_COLUMN = "_rank"
SOURCE_RANK_COLUMN = "_source_rank"
MAX_DECIMAL_PLACES = 6
DECIMAL_TOLERANCE = 1e-9
HISTORY_AFTER = pd.Timedelta(days=366)
FIRST = "first"
REVISION = "revision"
HISTORY = "history"
SERIES_PERIOD = ["series_id", "reference_period"]
SORT_COLUMNS = ["series_id", "frequency", "reference_period", "vintage_date"]


def collapse_to_changes(observations: pd.DataFrame) -> pd.DataFrame:
    """Keeps only the rows where a value is first published or changes.

    Where several rows share a series, reference period and vintage date, the
    one with the lowest value in the optional "_rank" column wins (ties go to the
    last row given, so a same-day correction beats the original).

    Args:
        observations: Rows with at least the key columns and "value".

    Returns:
        The change-only rows, sorted by series, period and vintage date.
    """
    if observations.empty:
        return observations
    deduplicated = _keep_best_row_per_key(observations)
    ordered = deduplicated.sort_values(SORT_COLUMNS, kind="stable")
    rounded = ordered["value"].round(VALUE_COMPARISON_DECIMALS)
    previous = rounded.groupby(
        [ordered["series_id"], ordered["reference_period"]], observed=True
    ).shift()
    is_change = previous.isna() | (rounded != previous)
    return ordered.loc[is_change].reset_index(drop=True)


def label_release_types(changes: pd.DataFrame) -> pd.DataFrame:
    """Says whether each change-only row is a first release, a revision or history.

    A period's first row is normally its first estimate. But where a source
    only begins long after a period ended (pre-1992 GDP growth first appears in
    the page history of May 2015, for example), that row is old history seen for
    the first time, not a release. A first row dated more than a year after its
    period ended is labelled "history"; later rows are "revision".

    Args:
        changes: Change-only rows with "series_id", "frequency",
            "reference_period" (as strings) and "vintage_date".

    Returns:
        The rows with a "release_type" column.
    """
    ordered = changes.sort_values(SORT_COLUMNS, kind="stable")
    is_first = ~ordered.duplicated(subset=SERIES_PERIOD, keep="first")
    ends = period_ends(ordered["reference_period"])
    is_late = ordered["vintage_date"] - ends > HISTORY_AFTER
    release_type = pd.Series(REVISION, index=ordered.index).mask(is_first, FIRST)
    return ordered.assign(release_type=release_type.mask(is_first & is_late, HISTORY))


def period_ends(periods: pd.Series) -> pd.Series:
    """Gives midnight on the last day of each reference period.

    Args:
        periods: Reference periods as canonical strings.

    Returns:
        The period end dates.
    """
    unique = periods.astype(str).unique()
    ends = {text: string_to_period(text).end_time.normalize() for text in unique}
    return periods.astype(str).map(ends)


def prefer_best_sources(observations: pd.DataFrame) -> pd.DataFrame:
    """Uses each weaker source only in months that no stronger source covers.

    Sources are ranked by "_source_rank" (lower is stronger). A row is dropped
    when a stronger source has any vintage for the same series in the same
    calendar month, because both then describe the same release. This lets
    triangles fill the years before exact-date page histories begin (late
    2015), and also any month afterwards that the page histories miss. Only the
    source type counts here: every dataset page of the same source type is
    kept, because each one carries releases the others miss (GDP's first
    quarterly estimate is published only in the PN2 dataset, for example).

    Rows that fill a gap after a stronger source has begun are rounded to that
    source's decimal places, so that differences in rounding between sources
    do not show up as false revisions.

    Args:
        observations: Rows with "series_id", "vintage_date", "value" and
            "_source_rank" columns.

    Returns:
        The rows that survive.
    """
    month = observations["vintage_date"].dt.to_period("M")
    strongest_in_month = observations.groupby(
        [observations["series_id"], month], observed=True
    )[SOURCE_RANK_COLUMN].transform("min")
    keep = observations[SOURCE_RANK_COLUMN] == strongest_in_month
    dropped = int((~keep).sum())
    if dropped:
        logger.info(
            f"Dropped {dropped} rows from weaker sources in months stronger ones cover"
        )
    return _round_gap_fills(observations.loc[keep])


def _round_gap_fills(observations: pd.DataFrame) -> pd.DataFrame:
    """Rounds weaker-source rows dated after a stronger source has begun.

    Args:
        observations: Rows with "series_id", "vintage_date", "value" and
            "_source_rank" columns.

    Returns:
        The rows, with gap-filling values rounded to the decimal places of the
        series' strongest source.
    """
    cutoffs = _stronger_source_starts(observations)
    keys = pd.MultiIndex.from_arrays(
        [observations["series_id"], observations[SOURCE_RANK_COLUMN]]
    )
    cutoff = pd.Series(cutoffs.reindex(keys).to_numpy(), index=observations.index)
    fills_gap = cutoff.notna() & (observations["vintage_date"] >= cutoff)
    if not fills_gap.any():
        return observations
    places = observations.loc[fills_gap, "series_id"].map(
        _strongest_source_decimal_places(observations)
    )
    filled = observations.copy()
    filled.loc[fills_gap, "value"] = [
        round(value, int(digits))
        for value, digits in zip(
            observations.loc[fills_gap, "value"], places, strict=True
        )
    ]
    logger.info(f"Filled {len(places):,} rows from weaker sources into gaps")
    return filled


def _strongest_source_decimal_places(observations: pd.DataFrame) -> pd.Series:
    """Finds how many decimal places each series' strongest source publishes.

    Args:
        observations: Rows with "series_id", "value" and "_source_rank" columns.

    Returns:
        The number of decimal places, indexed by series ID.
    """
    strongest_rank = observations.groupby("series_id", observed=True)[
        SOURCE_RANK_COLUMN
    ].transform("min")
    strongest = observations.loc[observations[SOURCE_RANK_COLUMN] == strongest_rank]
    return strongest.groupby("series_id", observed=True)["value"].agg(decimal_places)


def decimal_places(values: pd.Series) -> int:
    """Finds the fewest decimal places that represent every value exactly.

    Args:
        values: The values.

    Returns:
        A number of decimal places, at most MAX_DECIMAL_PLACES.
    """
    return next(
        (
            places
            for places in range(MAX_DECIMAL_PLACES)
            if ((values - values.round(places)).abs() < DECIMAL_TOLERANCE).all()
        ),
        MAX_DECIMAL_PLACES,
    )


def _stronger_source_starts(observations: pd.DataFrame) -> pd.Series:
    """Gives, for each series and source rank, when a stronger source first appears.

    Args:
        observations: Rows with "series_id", "vintage_date" and "_source_rank".

    Returns:
        The earliest vintage date of any stronger source, indexed by series and
        source rank; missing for each series' strongest source.
    """
    starts = (
        observations.groupby(["series_id", SOURCE_RANK_COLUMN], observed=True)[
            "vintage_date"
        ]
        .min()
        .sort_index()
    )
    return starts.groupby(level="series_id", observed=True).transform(
        lambda series_starts: series_starts.cummin().shift()
    )


def write_archive(changes: pd.DataFrame, path: Path = ARCHIVE_PATH) -> pd.DataFrame:
    """Checks the change-only table and writes it to parquet in a fixed order.

    Args:
        changes: The change-only table, with reference periods as strings.
        path: Where to write the parquet file.

    Returns:
        The table exactly as written.
    """
    conformed = (
        conform(changes).sort_values(SORT_COLUMNS, kind="stable").reset_index(drop=True)
    )
    validate(conformed)
    path.parent.mkdir(parents=True, exist_ok=True)
    conformed.to_parquet(path, index=False, engine="pyarrow")
    logger.info(
        f"Wrote {len(conformed):,} rows for {conformed['series_id'].nunique()} series to {path}"
    )
    return conformed


def read_archive(path: Path = ARCHIVE_PATH) -> pd.DataFrame:
    """Reads the archive, turning reference periods into pd.Period objects.

    One table holds every frequency, so the reference_period column has object
    dtype. Use to_period_dtype on a single-frequency subset for a PeriodDtype.

    Args:
        path: The parquet file to read.

    Returns:
        The archive.
    """
    archive = pd.read_parquet(path, engine="pyarrow")
    archive["reference_period"] = periods_from_strings(archive["reference_period"])
    return archive


def periods_from_strings(texts: pd.Series) -> pd.Series:
    """Converts canonical period strings to pd.Period objects, parsing each distinct value once.

    Args:
        texts: Strings such as "2024", "2024Q1" or "2024-03".

    Returns:
        An object-dtype series of pd.Period values.
    """
    lookup = {text: string_to_period(text) for text in pd.unique(texts.astype(str))}
    return texts.astype(str).map(lookup).astype(object)


def to_period_dtype(periods: pd.Series) -> pd.Series:
    """Gives a true PeriodDtype series from periods that share one frequency.

    Args:
        periods: pd.Period values of a single frequency.

    Returns:
        The same periods with a PeriodDtype.

    Raises:
        ValueError: If the periods mix frequencies.
    """
    frequencies = {period.freqstr for period in periods}
    if len(frequencies) != 1:
        raise ValueError(f"Expected one frequency, found {sorted(frequencies)}")
    return periods.astype(pd.PeriodDtype(frequencies.pop()))


def convert_frequency(
    periods: pd.Series, frequency: str, how: str = "end"
) -> pd.Series:
    """Maps periods to another frequency, for example months to the quarters containing them.

    Args:
        periods: pd.Period values, of any mix of frequencies.
        frequency: The target frequency: "A", "Q" or "M".
        how: For conversion to a finer frequency, use the "start" or "end" sub-period.

    Returns:
        The converted periods.
    """
    pandas_frequency = FREQUENCY_TO_PANDAS[frequency]
    return periods.map(lambda period: period.asfreq(pandas_frequency, how=how))


def as_of(archive: pd.DataFrame, date: str | pd.Timestamp) -> pd.DataFrame:
    """Rebuilds the data exactly as it stood on a given date.

    Args:
        archive: The change-only archive.
        date: The date of interest.

    Returns:
        For each series and reference period published by that date, the latest
        value known at that date.
    """
    known = archive.loc[archive["vintage_date"] <= pd.Timestamp(date)]
    return (
        known.sort_values("vintage_date", kind="stable")
        .groupby(SERIES_PERIOD, observed=True, sort=False)
        .tail(1)
        .reset_index(drop=True)
    )


def revision_triangle(archive: pd.DataFrame, series_id: str) -> pd.DataFrame:
    """Builds a full revisions triangle for one series.

    Args:
        archive: The change-only archive.
        series_id: The series to show.

    Returns:
        A table with one row per vintage date and one column per reference
        period, holding the value as published at each vintage.
    """
    series = archive.loc[archive["series_id"] == series_id]
    triangle = series.pivot_table(
        index="vintage_date",
        columns="reference_period",
        values="value",
        aggfunc="last",
        observed=True,
    )
    return triangle.sort_index().ffill()


def frequency_of_periods(periods: pd.Series) -> pd.Series:
    """Gives the "A", "Q" or "M" code for each pd.Period.

    Args:
        periods: pd.Period values.

    Returns:
        The frequency codes.
    """
    return periods.map(lambda period: PANDAS_TO_FREQUENCY[period.freqstr])


def _keep_best_row_per_key(observations: pd.DataFrame) -> pd.DataFrame:
    """Keeps one row per series, reference period and vintage date.

    Args:
        observations: Rows with the key columns and optionally "_rank".

    Returns:
        One row per key: lowest rank first, then the last row given.
    """
    with_order = observations.assign(_order=range(len(observations)))
    rank = with_order[RANK_COLUMN] if RANK_COLUMN in with_order else 0
    with_order = with_order.assign(
        _sort_rank=rank, _reverse_order=-with_order["_order"]
    )
    best_first = with_order.sort_values(["_sort_rank", "_reverse_order"], kind="stable")
    kept = best_first.drop_duplicates(subset=KEY_COLUMNS, keep="first")
    return kept.sort_values("_order").drop(
        columns=["_order", "_sort_rank", "_reverse_order"]
    )
