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


def prefer_best_sources(observations: pd.DataFrame) -> pd.DataFrame:
    """Drops rows from weaker sources once any stronger source starts covering a series.

    Each source type only fills the years before every stronger source for the
    same series begins. For example, once exact-date series histories begin (late
    2015), triangle rows for the same series from that date onwards are dropped,
    and real-time database rows are dropped once the triangle begins. This stops
    differences in rounding between sources showing up as false revisions. Only
    the source type counts here: every dataset page of the same source type is
    kept, because each one carries releases the others miss (GDP's first quarterly
    estimate is published only in the PN2 dataset, for example).

    Args:
        observations: Rows with "series_id", "vintage_date" and "_source_rank"
            columns.

    Returns:
        The rows that survive.
    """
    cutoffs = _stronger_source_starts(observations)
    keys = pd.MultiIndex.from_arrays(
        [observations["series_id"], observations[SOURCE_RANK_COLUMN]]
    )
    cutoff = pd.Series(cutoffs.reindex(keys).to_numpy(), index=observations.index)
    keep = cutoff.isna() | (observations["vintage_date"] < cutoff)
    dropped = int((~keep).sum())
    if dropped:
        logger.info(
            f"Dropped {dropped} rows from weaker sources that overlap stronger ones"
        )
    return observations.loc[keep]


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
