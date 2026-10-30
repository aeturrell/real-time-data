"""A per-series diagnostics report on coverage and quality of the archive."""

import pandas as pd

GAP_TOLERANCE = 1.5
TYPICAL_GAP_DAYS = {"M": 31, "Q": 92, "A": 366}


def coverage_report(archive: pd.DataFrame) -> pd.DataFrame:
    """Summarises, for each series, how many vintages it has and how well dated they are.

    Args:
        archive: The change-only archive.

    Returns:
        One row per series with vintage counts, date ranges, the share of
        vintages dated to the day, the longest gap between vintages, whether
        that gap is unusually long for the series' frequency, and the sources used.
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
    finest_frequency = _finest_frequency(group["frequency"].astype(str))
    longest_gap = int(gaps.max()) if gaps.notna().any() else 0
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
        "longest_gap_days": longest_gap,
        "unusual_gap": longest_gap > GAP_TOLERANCE * TYPICAL_GAP_DAYS[finest_frequency],
        "sources": ", ".join(sorted(group["source"].astype(str).unique())),
    }


def _finest_frequency(frequencies: pd.Series) -> str:
    """Gives the most frequent (finest) frequency present.

    Args:
        frequencies: Frequency codes.

    Returns:
        "M", "Q" or "A".
    """
    present = set(frequencies.unique())
    return next(frequency for frequency in ("M", "Q", "A") if frequency in present)
