"""Builds the data and markup for the landing page's tape-stack animation.

The animation is redrawn from the archive every time the site is rendered, so
it always shows every vintage the archive holds.
"""

import json

import pandas as pd

from realtime import revision_triangle

HERO_SERIES = "IHYQ"
HERO_FIRST_QUARTER = pd.Period("1988Q1", "Q")
QUARTERLY = "Q"
DECIMAL_PLACES = 2
DATA_ELEMENT_ID = "tape-hero-data"
GROWTH_LIMIT = 2.5
SCRIPT_PATH = "assets/tape_hero.js"


def vintage_snapshots(
    archive: pd.DataFrame,
    series_id: str = HERO_SERIES,
    first_quarter: pd.Period = HERO_FIRST_QUARTER,
) -> pd.DataFrame:
    """Gives the full history of one quarterly series as published on each vintage date.

    Args:
        archive: The change-only archive, as returned by read_archive.
        series_id: The quarterly series to show.
        first_quarter: The earliest quarter to keep.

    Returns:
        A table with one row per vintage date and one column per quarter.

    Raises:
        ValueError: If the archive holds no quarterly values for the series.
    """
    quarterly = archive.loc[
        (archive["series_id"] == series_id) & (archive["frequency"] == QUARTERLY)
    ]
    if quarterly.empty:
        raise ValueError(f"The archive holds no quarterly values for {series_id}.")
    triangle = revision_triangle(quarterly, series_id)
    kept_quarters = [
        quarter for quarter in triangle.columns if quarter >= first_quarter
    ]
    return triangle[kept_quarters]


def published_values(row: pd.Series) -> list[float | None]:
    """Turns one vintage into a list of values, dropping quarters not yet published.

    Args:
        row: One vintage of the snapshot table.

    Returns:
        The values in quarter order, with None for gaps and no trailing gaps.
    """
    values = [
        None if pd.isna(value) else round(float(value), DECIMAL_PLACES) for value in row
    ]
    while values and values[-1] is None:
        values.pop()
    return values


def hero_payload(snapshots: pd.DataFrame, series_id: str = HERO_SERIES) -> dict:
    """Packs the snapshot table into the shape the animation script reads.

    Args:
        snapshots: The table from vintage_snapshots.
        series_id: The series the table holds.

    Returns:
        A dictionary ready to be written as JSON.
    """
    return {
        "seriesId": series_id,
        "growthLimit": GROWTH_LIMIT,
        "quarters": [str(quarter) for quarter in snapshots.columns],
        "vintages": [
            {"date": f"{vintage_date:%Y-%m-%d}", "values": published_values(row)}
            for vintage_date, row in snapshots.iterrows()
        ],
    }


def hero_caption(snapshots: pd.DataFrame) -> str:
    """Writes the plain-English caption shown under the animation.

    Args:
        snapshots: The table from vintage_snapshots.

    Returns:
        The caption text.
    """
    first_year = snapshots.index.min().year
    return (
        f"Every published estimate of UK GDP growth since {first_year}: "
        f"{len(snapshots):,} vintages. Each strip of tape is the full history as published on one date. "
        "Amber marks the quarters revised in that release; white marks first estimates. "
        f"Bars are cut off at ±{GROWTH_LIMIT}%: each amber stripe on a cut-off bar stands for "
        f"a further {GROWTH_LIMIT} points, and the true value is printed beside it."
    )


def hero_markup(archive: pd.DataFrame) -> str:
    """Builds the raw HTML block that embeds the animation in a Quarto page.

    Args:
        archive: The change-only archive, as returned by read_archive.

    Returns:
        A raw HTML block for a Quarto cell with `output: asis`.
    """
    snapshots = vintage_snapshots(archive)
    payload = json.dumps(hero_payload(snapshots), separators=(",", ":")).replace(
        "</", "<\\/"
    )
    return f"""```{{=html}}
<figure class="tape-hero">
<div class="tape-hero-screen scan-panel"><canvas role="img" aria-label="Animation of {len(snapshots):,} vintages of UK GDP growth stacking up as strips of tape"></canvas></div>
<figcaption><span>{hero_caption(snapshots)}</span><button type="button" class="tape-hero-replay">Replay</button></figcaption>
</figure>
<script type="application/json" id="{DATA_ELEMENT_ID}">{payload}</script>
<script src="{SCRIPT_PATH}" defer></script>
```"""
