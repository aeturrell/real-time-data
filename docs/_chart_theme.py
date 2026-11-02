"""Shared colours, fonts and helpers that give every docs chart the same look."""

from pathlib import Path

import matplotlib as mpl
import pandas as pd
from cycler import cycler
from matplotlib import font_manager
from matplotlib.axes import Axes

STATIC_GREY = "#1C1E24"
CORRUPTED_VIOLET = "#5C2483"
SCAN_LINE_CYAN = "#00F5D4"
DEGAUSS_MAGENTA = "#FF2A85"
PHOSPHOR_AMBER = "#FFB703"
ELECTRON_WHITE = "#F1F5F9"
OXIDE_SLATE = "#64748B"
SECONDARY_TEXT = "#BABFC7"
GRID_LINE = "#2E3039"

FONT_DIRECTORY = Path(__file__).parent / "assets" / "fonts"
BODY_FONT = "Space Mono"
TAG_FONT = "Silkscreen"
BASE_FONT_SIZE = 9
TAG_FONT_SIZE = 8
TAG_VERTICAL_POSITION = 1.02
HAIRLINE_WIDTH = 0.6
QUARTER_WORDS = {1: "first", 2: "second", 3: "third", 4: "fourth"}


def apply_chart_theme() -> None:
    """Registers the bundled fonts and sets matplotlib's defaults to the site theme."""
    for font_path in FONT_DIRECTORY.glob("*.ttf"):
        font_manager.fontManager.addfont(font_path)
    mpl.rcParams.update(
        {
            "figure.facecolor": STATIC_GREY,
            "savefig.facecolor": STATIC_GREY,
            "axes.facecolor": STATIC_GREY,
            "axes.edgecolor": OXIDE_SLATE,
            "axes.labelcolor": SECONDARY_TEXT,
            "axes.linewidth": HAIRLINE_WIDTH,
            "axes.grid": True,
            "axes.axisbelow": True,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.prop_cycle": cycler(
                color=[SCAN_LINE_CYAN, DEGAUSS_MAGENTA, PHOSPHOR_AMBER, OXIDE_SLATE]
            ),
            "grid.color": GRID_LINE,
            "grid.linewidth": HAIRLINE_WIDTH,
            "font.family": BODY_FONT,
            "font.size": BASE_FONT_SIZE,
            "text.color": ELECTRON_WHITE,
            "xtick.color": SECONDARY_TEXT,
            "ytick.color": SECONDARY_TEXT,
            "xtick.major.width": HAIRLINE_WIDTH,
            "ytick.major.width": HAIRLINE_WIDTH,
            "legend.frameon": False,
            "svg.fonttype": "path",
        }
    )


def add_chart_tag(axes: Axes, text: str) -> None:
    """Writes a small bitmap-style label above the top-left corner of a chart.

    Args:
        axes: The axes to label.
        text: The label, shown in capitals.
    """
    axes.text(
        0,
        TAG_VERTICAL_POSITION,
        text.upper(),
        transform=axes.transAxes,
        color=PHOSPHOR_AMBER,
        fontfamily=TAG_FONT,
        fontsize=TAG_FONT_SIZE,
    )


def quarter_in_words(period: pd.Period) -> str:
    """Writes a quarter out in words, such as "second quarter of 2020".

    Args:
        period: A quarterly period.

    Returns:
        The quarter in words.
    """
    return f"{QUARTER_WORDS[period.quarter]} quarter of {period.year}"


def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    """Builds a Markdown table for a cell whose output is treated as Markdown.

    Args:
        headers: The column headings.
        rows: The table's rows, already formatted as text.

    Returns:
        The table as Markdown text.
    """
    lines = [headers, ["---"] * len(headers), *rows]
    return "\n".join(f"| {' | '.join(line)} |" for line in lines)


