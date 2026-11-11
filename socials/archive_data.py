"""Turns the archive into the tables the social videos and stills draw."""

import numpy as np
import pandas as pd

from realtime import revision_triangle

QUARTERLY = "Q"
QUARTERS_PER_YEAR = 4
DAYS_PER_YEAR = 365.25


def parse_date(text: str | None) -> pd.Timestamp | None:
    """Reads a YYYY-MM-DD setting.

    Args:
        text: The setting, or None if it was left out.

    Returns:
        The date, or None.

    Raises:
        ValueError: If the text is not a date.
    """
    return None if text is None else pd.Timestamp(text)


def parse_quarter(text: str | None) -> pd.Period | None:
    """Reads a YYYYQn setting.

    Args:
        text: The setting, or None if it was left out.

    Returns:
        The quarter, or None.

    Raises:
        ValueError: If the text is not a quarter.
    """
    return None if text is None else pd.Period(text, QUARTERLY)


def between(
    values: pd.Index | pd.Series,
    first: pd.Timestamp | pd.Period | None,
    last: pd.Timestamp | pd.Period | None,
) -> np.ndarray:
    """Marks which values fall between two optional bounds, inclusive.

    Args:
        values: Dates or quarters.
        first: The lower bound, or None for no lower bound.
        last: The upper bound, or None for no upper bound.

    Returns:
        True for each value inside the bounds, in the same order as the values.
    """
    series = pd.Series(list(values))
    inside = np.ones(len(series), dtype=bool)
    if first is not None:
        inside &= (series >= first).to_numpy()
    if last is not None:
        inside &= (series <= last).to_numpy()
    return inside


def vintage_years(dates: pd.Index | pd.Series) -> dict[str, int]:
    """Gives the first and last year of a set of dates, for headline placeholders.

    Args:
        dates: Vintage or publication dates.

    Returns:
        The placeholders first_year and last_year.
    """
    return {
        "first_year": pd.Timestamp(min(dates)).year,
        "last_year": pd.Timestamp(max(dates)).year,
    }


def quarterly_triangle(archive: pd.DataFrame, series_id: str) -> pd.DataFrame:
    """Gives the full history of a quarterly series as published on each vintage date.

    Args:
        archive: The change-only archive.
        series_id: The series to show.

    Returns:
        One row per vintage date and one column per quarter.

    Raises:
        ValueError: If the archive has no quarterly values for the series.
    """
    quarterly = archive.loc[
        (archive["series_id"] == series_id) & (archive["frequency"] == QUARTERLY)
    ]
    if quarterly.empty:
        raise ValueError(f"The archive holds no quarterly values for {series_id}.")
    return revision_triangle(quarterly, series_id)


def first_estimate_cells(triangle: pd.DataFrame) -> pd.DataFrame:
    """Marks the cells where a quarter was published for the first time.

    Args:
        triangle: A table from quarterly_triangle.

    Returns:
        True where a value first appears.
    """
    return triangle.notna() & triangle.shift(1).isna()


def revised_cells(triangle: pd.DataFrame) -> pd.DataFrame:
    """Marks the cells where a published value changed.

    Args:
        triangle: A table from quarterly_triangle.

    Returns:
        True where a value differs from the one published before it.
    """
    previous = triangle.shift(1)
    return triangle.notna() & previous.notna() & triangle.ne(previous)


def first_estimates(triangle: pd.DataFrame) -> pd.Series:
    """Gives the first published value of every quarter.

    Args:
        triangle: A table from quarterly_triangle.

    Returns:
        One value per quarter.
    """
    return triangle.bfill().iloc[0]


def window(
    table: pd.DataFrame,
    first_quarter: pd.Period | None = None,
    last_quarter: pd.Period | None = None,
    first_vintage: pd.Timestamp | None = None,
    last_vintage: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Keeps only the quarters and vintage dates inside the chosen bounds.

    Args:
        table: A table with vintage dates as rows and quarters as columns.
        first_quarter: The earliest quarter to keep.
        last_quarter: The latest quarter to keep.
        first_vintage: The earliest vintage date to keep.
        last_vintage: The latest vintage date to keep.

    Returns:
        The trimmed table.

    Raises:
        ValueError: If nothing is left after trimming.
    """
    rows = between(table.index, first_vintage, last_vintage)
    columns = between(table.columns, first_quarter, last_quarter)
    trimmed = table.loc[rows, columns]
    if trimmed.empty:
        raise ValueError(
            "No data is left inside the chosen quarters and vintage dates."
        )
    return trimmed


def quarter_position(quarter: pd.Period) -> float:
    """Places a quarter on a scale of years, so 2020 Q3 becomes 2020.5.

    Args:
        quarter: A quarterly period.

    Returns:
        The start of the quarter in years.
    """
    return quarter.year + (quarter.quarter - 1) / QUARTERS_PER_YEAR


def date_position(date: pd.Timestamp) -> float:
    """Places a date on a scale of years, so 1 July 2020 becomes about 2020.5.

    Args:
        date: A date.

    Returns:
        The date in years.
    """
    return date.year + (date.dayofyear - 1) / DAYS_PER_YEAR


def quarter_label(quarter: pd.Period) -> str:
    """Writes a quarter as, for example, "1997 Q1".

    Args:
        quarter: A quarterly period.

    Returns:
        The label.
    """
    return f"{quarter.year} Q{quarter.quarter}"


def series_title(archive: pd.DataFrame, series_id: str) -> str:
    """Gives a series' published title.

    Args:
        archive: The archive.
        series_id: The series.

    Returns:
        The title, or the series code if there is none.
    """
    titles = archive.loc[archive["series_id"] == series_id, "title"].dropna()
    return str(titles.iloc[-1]) if not titles.empty else series_id


def window_bounds(settings: object) -> dict:
    """Reads the optional quarter and vintage bounds from a card's settings.

    Args:
        settings: Any settings object; bounds it does not have are left open.

    Returns:
        The bounds, ready to pass to window.
    """
    return {
        "first_quarter": parse_quarter(getattr(settings, "first_quarter", None)),
        "last_quarter": parse_quarter(getattr(settings, "last_quarter", None)),
        "first_vintage": parse_date(getattr(settings, "first_vintage", None)),
        "last_vintage": parse_date(getattr(settings, "last_vintage", None)),
    }


def quarter_estimates(
    triangle: pd.DataFrame,
    quarter: pd.Period,
    first_vintage: pd.Timestamp | None,
    last_vintage: pd.Timestamp | None,
) -> pd.Series:
    """Gives each distinct estimate of one quarter, dated by when it was published.

    Args:
        triangle: A table from quarterly_triangle.
        quarter: The quarter to follow.
        first_vintage: The earliest publication date to keep.
        last_vintage: The latest publication date to keep.

    Returns:
        One value per estimate, indexed by publication date.

    Raises:
        ValueError: If the quarter has no estimates inside the chosen dates.
    """
    if quarter not in triangle.columns:
        raise ValueError(f"There are no estimates for {quarter_label(quarter)}.")
    history = triangle[quarter].dropna()
    estimates = history[history.ne(history.shift())]
    estimates = estimates[between(estimates.index, first_vintage, last_vintage)]
    if estimates.empty:
        raise ValueError(
            f"No estimates of {quarter_label(quarter)} fall inside the chosen dates."
        )
    return estimates


def release_links(
    triangle: pd.DataFrame, bounds: dict
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Marks first estimates and revisions, keeping only quarters touched inside the bounds.

    Args:
        triangle: A table from quarterly_triangle.
        bounds: Quarter and vintage bounds, as from window_bounds.

    Returns:
        The first-estimate cells and the revised cells, with matching rows and columns.
    """
    first_cells = window(first_estimate_cells(triangle), **bounds)
    revised = window(revised_cells(triangle), **bounds)
    linked_quarters = first_cells.columns[(first_cells | revised).any().to_numpy()]
    return first_cells[linked_quarters], revised[linked_quarters]
