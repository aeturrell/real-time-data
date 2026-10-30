"""A per-series diagnostics report on coverage and quality of the archive."""

import pandas as pd

GAP_TOLERANCE = 3


def coverage_report(archive: pd.DataFrame) -> pd.DataFrame:
    """Summarises, for each series, how many vintages it has and how well dated they are.

    Args:
        archive: The change-only archive.

    Returns:
        One row per series with vintage counts, date ranges, the share of
        vintages dated to the day, the usual (median) and longest gaps between
        vintages, when the longest gap ended, whether it is more than three
        times the usual gap, and the sources used.
    """
    rows = [
        _series_summary(series_id, group)
        for series_id, group in archive.groupby("series_id", observed=True)
    ]
    return pd.DataFrame(rows).sort_values("series_id").reset_index(drop=True)


def _series_summary(series_id: str, group: pd.DataFrame) -> dict[str, object]:
    """Summarises one series.

    Args:
        series_id: The series.
        group: Its rows.

    Returns:
        The summary values.
    """
    vintages = group.drop_duplicates("vintage_date").sort_values("vintage_date")
    gaps = vintages["vintage_date"].diff().dt.days
    has_gaps = gaps.notna().any()
    usual_gap = int(gaps.median()) if has_gaps else 0
    longest_gap = int(gaps.max()) if has_gaps else 0
    longest_gap_ends = (
        vintages["vintage_date"].loc[gaps.idxmax()].date() if has_gaps else None
    )
    return {
        "series_id": series_id,
        "short_name": group["short_name"].iloc[0],
        "rows": len(group),
        "vintages": len(vintages),
        "first_vintage": vintages["vintage_date"].min().date(),
        "last_vintage": vintages["vintage_date"].max().date(),
        "share_day_precision": round(
            (vintages["vintage_date_precision"] == "day").mean(), 3
        ),
        "usual_gap_days": usual_gap,
        "longest_gap_days": longest_gap,
        "longest_gap_ends": longest_gap_ends,
        "unusual_gap": longest_gap > GAP_TOLERANCE * usual_gap,
        "sources": ", ".join(sorted(group["source"].astype(str).unique())),
    }
