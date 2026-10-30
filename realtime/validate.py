"""Checks that the assembled archive is internally consistent.

Two checks are run:

- No look-ahead: a value can never have been published on or before the last day
  of the period it describes, so any such row points to a wrongly dated vintage.
- Cross-product agreement: ONS publishes some series both as levels (in page
  histories and real-time databases) and as growth-rate revisions triangles. At
  every triangle vintage, growth worked out from the levels known at that date
  should match the published growth rate to within rounding.
"""

import pandas as pd

from realtime.store import revision_triangle

GROWTH_PAIRS = {
    "IHYQ": "ABMI",
    "YBHA_QOQ": "YBHA",
    "ABJR_QOQ": "ABJR",
    "NPQT_QOQ": "NPQT",
    "NMRY_QOQ": "NMRY",
    "IKBK_QOQ": "IKBK",
    "IKBL_QOQ": "IKBL",
}
QUARTERLY = "Q"
PERCENT = 100
ROUNDING_TOLERANCE = 0.05 + 1e-9
CLOSE_TOLERANCE = 0.15


def look_ahead_summary(archive: pd.DataFrame) -> pd.DataFrame:
    """Counts rows dated on or before the end of their own reference period.

    Args:
        archive: The archive, as returned by read_archive.

    Returns:
        One row per series and source with any such rows, with the count and the
        first example.
    """
    period_ends = archive["reference_period"].map(
        lambda period: period.end_time.normalize()
    )
    early = archive.loc[archive["vintage_date"] <= period_ends]
    if early.empty:
        return pd.DataFrame(columns=["series_id", "source", "rows", "example"])
    return (
        early.groupby(["series_id", "source"], observed=True)
        .apply(
            lambda rows: pd.Series(
                {
                    "rows": len(rows),
                    "example": f"{rows['reference_period'].iloc[0]} published "
                    f"{rows['vintage_date'].iloc[0]:%Y-%m-%d}",
                }
            ),
            include_groups=False,
        )
        .reset_index()
    )


def growth_agreement_report(archive: pd.DataFrame) -> pd.DataFrame:
    """Compares each growth triangle with growth worked out from its level series.

    Args:
        archive: The archive, as returned by read_archive.

    Returns:
        One row per growth series with the number of values compared, the share
        matching to within rounding, the share within 0.15 percentage points,
        and the largest difference found.
    """
    quarterly = archive.loc[archive["frequency"] == QUARTERLY]
    rows = [
        growth_agreement(quarterly, growth_id, level_id)
        for growth_id, level_id in GROWTH_PAIRS.items()
    ]
    return pd.DataFrame(rows)


def growth_agreement(
    quarterly: pd.DataFrame, growth_id: str, level_id: str
) -> dict[str, object]:
    """Compares one growth triangle with growth implied by the levels known at each vintage.

    Args:
        quarterly: The quarterly rows of the archive.
        growth_id: The growth-rate series, for example "IHYQ".
        level_id: The level series, for example "ABMI".

    Returns:
        The comparison summary.
    """
    published = revision_triangle(quarterly, growth_id)
    implied = _implied_growth(revision_triangle(quarterly, level_id), published.index)
    summary: dict[str, object] = {"growth": growth_id, "levels": level_id}
    if published.empty or implied.empty:
        return summary | {"compared": 0}
    published, implied = published.align(implied, join="inner")
    differences = (implied - published).abs().stack().dropna()
    if differences.empty:
        return summary | {"compared": 0}
    worst_vintage, worst_period = differences.idxmax()
    return summary | {
        "compared": len(differences),
        "share_within_rounding": round((differences <= ROUNDING_TOLERANCE).mean(), 3),
        "share_within_0.15": round((differences <= CLOSE_TOLERANCE).mean(), 3),
        "largest_difference": round(differences.max(), 2),
        "largest_at": f"{worst_period} as of {worst_vintage:%Y-%m-%d}",
    }


def _implied_growth(levels: pd.DataFrame, dates: pd.Index) -> pd.DataFrame:
    """Works out quarter-on-quarter growth from the levels known at each date.

    Args:
        levels: A revisions triangle of levels (vintage dates by quarters).
        dates: The dates at which to work out growth.

    Returns:
        Percentage growth, with one row per date and one column per quarter.
    """
    if levels.empty:
        return levels
    quarters = pd.period_range(
        min(levels.columns), max(levels.columns), freq=levels.columns[0].freq
    )
    known = (
        levels.reindex(columns=quarters)
        .reindex(levels.index.union(dates))
        .ffill()
        .loc[dates]
    )
    return (known / known.shift(1, axis=1) - 1) * PERCENT
