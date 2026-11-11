"""Builds the data and on-screen text each video needs from the archive.

Each scene function returns what the matching script in socials/animations draws.
"""

from collections.abc import Callable
from dataclasses import dataclass

import numpy as np
import pandas as pd

from docs import _hero as hero
from socials.archive_data import (
    between,
    first_estimate_cells,
    first_estimates,
    parse_date,
    parse_quarter,
    quarter_estimates,
    quarter_label,
    quarterly_triangle,
    release_links,
    series_title,
    vintage_years,
    window,
    window_bounds,
)
from socials.settings import (
    ArchiveStatsSettings,
    EstimatePathSettings,
    ProvenanceSettings,
    ReleaseCalendarSettings,
    RevisionHeatmapSettings,
    TapeStackSettings,
    TitleCardSettings,
    TwoClocksSettings,
    VintageOverlaySettings,
    WeekdayRhythmSettings,
)

DAYS_IN_WEEK = 7
HASH_CHARACTERS = 16
SHORT_HASH_CHARACTERS = 8
HASHES_PER_TRACK = 14
MONTH_INDEX_OFFSET = 1


@dataclass(frozen=True)
class Scene:
    """Everything one video needs.

    Attributes:
        payload: The data the animation script draws.
        card: The text around the animation: tag, headline, footer and note.
    """

    payload: dict
    card: dict


def card_text(
    footer: str,
    tag: str = "",
    headline: str = "",
    note: str = "",
    full_area: bool = False,
) -> dict:
    """Collects the text drawn around an animation.

    Args:
        footer: The text along the bottom left.
        tag: The small amber label above the headline.
        headline: The headline.
        note: The text along the bottom right.
        full_area: Whether the animation fills the card, with no tag or headline.

    Returns:
        The card text, in the shape the recorder reads.
    """
    return {
        "footer": footer,
        "tag": tag,
        "headline": headline,
        "note": note,
        "fullArea": full_area,
    }


def series_tag(archive: pd.DataFrame, series_id: str) -> str:
    """Writes the amber label naming a series.

    Args:
        archive: The archive.
        series_id: The series.

    Returns:
        The series code and title.
    """
    return f"{series_id} · {series_title(archive, series_id)}"


def tape_stack_scene(
    archive: pd.DataFrame, settings: TapeStackSettings, footer: str
) -> Scene:
    """Builds the tape stack video from the same data as the landing page.

    Args:
        archive: The change-only archive.
        settings: The video's settings.
        footer: The footer text.

    Returns:
        The scene.
    """
    snapshots = hero.vintage_snapshots(
        archive, settings.series_id, parse_quarter(settings.first_quarter)
    )
    headline = settings.headline.format(
        count=len(snapshots), **vintage_years(snapshots.index)
    )
    note = "Amber: quarters revised in each release. White: first estimates"
    return Scene(
        hero.hero_payload(snapshots, settings.series_id),
        card_text(footer, series_tag(archive, settings.series_id), headline, note),
    )


def estimate_path_scene(
    archive: pd.DataFrame, settings: EstimatePathSettings, footer: str
) -> Scene:
    """Builds the video that plays through every estimate of one quarter.

    Args:
        archive: The change-only archive.
        settings: The video's settings.
        footer: The footer text.

    Returns:
        The scene.
    """
    quarter = parse_quarter(settings.quarter)
    estimates = quarter_estimates(
        quarterly_triangle(archive, settings.series_id),
        quarter,
        parse_date(settings.first_vintage),
        parse_date(settings.last_vintage),
    )
    headline = settings.headline.format(
        quarter=quarter_label(quarter),
        count=len(estimates),
        **vintage_years(estimates.index),
    )
    payload = {
        "quarter": str(quarter),
        "estimates": [
            [f"{date:%Y-%m-%d}", round(float(value), 2)]
            for date, value in estimates.items()
        ],
    }
    note = f"{len(estimates)} estimates, in the order they were published"
    return Scene(
        payload,
        card_text(footer, series_tag(archive, settings.series_id), headline, note),
    )


def revision_heatmap_scene(
    archive: pd.DataFrame, settings: RevisionHeatmapSettings, footer: str
) -> Scene:
    """Builds the scrolling revision triangle video.

    Args:
        archive: The change-only archive.
        settings: The video's settings.
        footer: The footer text.

    Returns:
        The scene.
    """
    triangle = quarterly_triangle(archive, settings.series_id)
    bounds = window_bounds(settings)
    revisions = window(triangle - first_estimates(triangle), **bounds)
    first_cells = window(first_estimate_cells(triangle), **bounds)
    payload = {
        "quarters": [str(quarter) for quarter in revisions.columns],
        "colourLimit": settings.colour_limit,
        "vintages": [
            {
                "date": f"{vintage_date:%Y-%m-%d}",
                "revisions": hero.published_values(row),
                "first": np.flatnonzero(
                    first_cells.loc[vintage_date].to_numpy()
                ).tolist(),
            }
            for vintage_date, row in revisions.iterrows()
        ],
    }
    headline = settings.headline.format(
        count=len(revisions), **vintage_years(revisions.index)
    )
    note = f"Colour is at full strength at ±{settings.colour_limit} percentage points"
    return Scene(
        payload,
        card_text(footer, series_tag(archive, settings.series_id), headline, note),
    )


def vintage_overlay_scene(
    archive: pd.DataFrame, settings: VintageOverlaySettings, footer: str
) -> Scene:
    """Builds the video that draws every vintage on top of the others.

    Args:
        archive: The change-only archive.
        settings: The video's settings.
        footer: The footer text.

    Returns:
        The scene.
    """
    shown = window(
        quarterly_triangle(archive, settings.series_id), **window_bounds(settings)
    )
    payload = hero.hero_payload(shown, settings.series_id) | {
        "valueLimit": settings.value_limit
    }
    headline = settings.headline.format(count=len(shown), **vintage_years(shown.index))
    note = f"Axis cut at ±{settings.value_limit}; larger values run off the chart"
    return Scene(
        payload,
        card_text(footer, series_tag(archive, settings.series_id), headline, note),
    )


def two_clocks_scene(
    archive: pd.DataFrame, settings: TwoClocksSettings, footer: str
) -> Scene:
    """Builds the video linking each quarter to the dates it was published and revised.

    Args:
        archive: The change-only archive.
        settings: The video's settings.
        footer: The footer text.

    Returns:
        The scene.
    """
    first_cells, revised = release_links(
        quarterly_triangle(archive, settings.series_id), window_bounds(settings)
    )
    payload = {
        "quarters": [str(quarter) for quarter in first_cells.columns],
        "vintages": [
            {
                "date": f"{vintage_date:%Y-%m-%d}",
                "first": np.flatnonzero(
                    first_cells.loc[vintage_date].to_numpy()
                ).tolist(),
                "revised": np.flatnonzero(
                    revised.loc[vintage_date].to_numpy()
                ).tolist(),
            }
            for vintage_date in first_cells.index
        ],
    }
    headline = settings.headline.format(
        count=len(first_cells), **vintage_years(first_cells.index)
    )
    note = f"{len(first_cells)} releases, {first_cells.index[0]:%b %Y} to {first_cells.index[-1]:%b %Y}"
    return Scene(
        payload,
        card_text(footer, series_tag(archive, settings.series_id), headline, note),
    )


def archive_stats_scene(
    archive: pd.DataFrame, settings: ArchiveStatsSettings, footer: str
) -> Scene:
    """Builds the video of the archive's headline numbers rolling up on counters.

    Args:
        archive: The change-only archive.
        settings: The video's settings.
        footer: The footer text.

    Returns:
        The scene.
    """
    published = archive.loc[
        between(archive["vintage_date"], None, parse_date(settings.as_of))
    ]
    latest = published["vintage_date"].max()
    payload = {
        "tiles": [
            {
                "label": "series",
                "kind": "count",
                "value": int(published["series_id"].nunique()),
                "commas": False,
            },
            {
                "label": "published values and revisions",
                "kind": "count",
                "value": len(published),
                "commas": True,
            },
            {
                "label": "earliest publication held",
                "kind": "count",
                "value": int(published["vintage_date"].min().year),
                "commas": False,
            },
            {
                "label": "latest publication held",
                "kind": "month",
                "year": latest.year,
                "month": latest.month - MONTH_INDEX_OFFSET,
            },
        ]
    }
    headline = settings.headline.format(latest=f"{latest:%B %Y}")
    return Scene(
        payload,
        card_text(footer, "UK real-time data", headline, f"As of {latest:%d %B %Y}"),
    )


def release_calendar_scene(
    archive: pd.DataFrame, settings: ReleaseCalendarSettings, footer: str
) -> Scene:
    """Builds the video that scans every publication date like a barcode.

    Args:
        archive: The change-only archive.
        settings: The video's settings.
        footer: The footer text.

    Returns:
        The scene.

    Raises:
        ValueError: If no publication dates fall inside the chosen dates.
    """
    dates = archive["vintage_date"]
    counts = (
        dates[
            between(
                dates, parse_date(settings.first_date), parse_date(settings.last_date)
            )
        ]
        .value_counts()
        .sort_index()
    )
    if counts.empty:
        raise ValueError("No publication dates fall inside the chosen dates.")
    payload = {
        "days": [[f"{date:%Y-%m-%d}", int(count)] for date, count in counts.items()],
        "highlightAbove": settings.highlight_above,
    }
    headline = settings.headline.format(
        count=f"{len(counts):,}", **vintage_years(counts.index)
    )
    note = f"Line height: values published that day (log scale). Amber: over {settings.highlight_above:,}"
    return Scene(
        payload,
        card_text(footer, "UK real-time data · release calendar", headline, note),
    )


def weekday_rhythm_scene(
    archive: pd.DataFrame, settings: WeekdayRhythmSettings, footer: str
) -> Scene:
    """Builds the oscilloscope video of values published on each weekday.

    Args:
        archive: The change-only archive.
        settings: The video's settings.
        footer: The footer text.

    Returns:
        The scene.
    """
    dates = archive["vintage_date"]
    dates = dates[
        between(dates, parse_date(settings.first_date), parse_date(settings.last_date))
    ]
    counts = dates.dt.weekday.value_counts().reindex(range(DAYS_IN_WEEK), fill_value=0)
    payload = {"counts": [int(count) for count in counts]}
    headline = settings.headline.format(**vintage_years(dates))
    note = "Height of each burst: values published on that weekday"
    return Scene(
        payload, card_text(footer, "UK real-time data · release week", headline, note)
    )


def provenance_scene(
    archive: pd.DataFrame, settings: ProvenanceSettings, footer: str
) -> Scene:
    """Builds the video of source file fingerprints decoding into archive rows.

    Args:
        archive: The change-only archive.
        settings: The video's settings.
        footer: The footer text.

    Returns:
        The scene.

    Raises:
        ValueError: If no rows fall inside the chosen dates.
    """
    rows = archive.loc[
        between(
            archive["vintage_date"],
            parse_date(settings.first_vintage),
            parse_date(settings.last_vintage),
        )
    ]
    if rows.empty:
        raise ValueError("No archive rows fall inside the chosen dates.")
    generator = np.random.default_rng(settings.seed)
    fingerprints = rows["content_sha256"].astype(str).unique()
    tracks = [
        "  ".join(
            fingerprint[:HASH_CHARACTERS]
            for fingerprint in generator.choice(fingerprints, HASHES_PER_TRACK)
        )
        for _ in range(settings.track_count)
    ]
    records = [
        [
            str(record.content_sha256)[:SHORT_HASH_CHARACTERS],
            f"{record.series_id}  {record.reference_period}  published {record.vintage_date:%Y-%m-%d}  value {record.value:g}",
        ]
        for record in rows.iloc[
            generator.choice(len(rows), settings.record_count, replace=False)
        ].itertuples()
    ]
    headline = settings.headline.format(count=f"{len(fingerprints):,}")
    note = "Amber: SHA-256 fingerprints of source files. Boxed: archive rows and their file"
    return Scene(
        {"tracks": tracks, "records": records},
        card_text(footer, "UK real-time data · provenance", headline, note),
    )


def title_card_scene(
    archive: pd.DataFrame, settings: TitleCardSettings, footer: str
) -> Scene:
    """Builds the title video.

    Args:
        archive: The change-only archive, which this video does not use.
        settings: The video's settings.
        footer: The footer text.

    Returns:
        The scene.
    """
    return Scene(
        {"title": settings.title, "subtitle": settings.subtitle},
        card_text(footer, full_area=True),
    )


VIDEOS: dict[str, tuple[type, Callable[..., Scene]]] = {
    "tape_stack": (TapeStackSettings, tape_stack_scene),
    "estimate_path": (EstimatePathSettings, estimate_path_scene),
    "revision_heatmap": (RevisionHeatmapSettings, revision_heatmap_scene),
    "vintage_overlay": (VintageOverlaySettings, vintage_overlay_scene),
    "two_clocks": (TwoClocksSettings, two_clocks_scene),
    "archive_stats": (ArchiveStatsSettings, archive_stats_scene),
    "release_calendar": (ReleaseCalendarSettings, release_calendar_scene),
    "weekday_rhythm": (WeekdayRhythmSettings, weekday_rhythm_scene),
    "provenance": (ProvenanceSettings, provenance_scene),
    "title_card": (TitleCardSettings, title_card_scene),
}
