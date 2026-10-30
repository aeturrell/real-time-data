"""Turning the many ways ONS labels time periods into proper pandas periods.

Reference periods are stored as strings that pd.Period reads unambiguously:
"2024" (annual), "2024Q1" (quarterly) and "2024-03" (monthly).
"""

import re
from datetime import datetime

import pandas as pd

FREQUENCY_TO_PANDAS = {"A": "Y-DEC", "Q": "Q-DEC", "M": "M"}
PANDAS_TO_FREQUENCY = {"Y-DEC": "A", "Q-DEC": "Q", "M": "M"}

QUARTER_PATTERN = re.compile(r"^(\d{4})\s*Q([1-4])$")
QUARTER_FIRST_PATTERN = re.compile(r"^Q([1-4])\s+(\d{4})$")
YEAR_MONTH_PATTERN = re.compile(r"^(\d{4})\s+([A-Z]{3})$")
MONTH_YEAR_PATTERN = re.compile(r"^([A-Z]{3})-(\d{2})$")
MONTH_FULL_YEAR_PATTERN = re.compile(r"^([A-Z]{3})\s+(\d{4})$")
YEAR_PATTERN = re.compile(r"^(\d{4})$")
THREE_MONTH_RANGE_PATTERN = re.compile(r"^(\d{4})\s+([A-Z]{3})-([A-Z]{3})$")
EARLIEST_NUMERIC_YEAR = 1900
LATEST_NUMERIC_YEAR = 2100


def parse_period_label(label: object, frequency: str) -> pd.Period | None:
    """Reads a single ONS period label as a pandas period of the given frequency.

    Handles datetimes, "2024 Q1", "2024Q1", "Q1 2024", "2024 MAR", "Mar-24",
    "Mar 2024" and "2024",
    and whole-number years (2024 or 2024.0, as spreadsheets store them).
    A monthly label read at quarterly frequency gives the quarter containing it.

    Args:
        label: The raw label, as found in a spreadsheet or JSON document.
        frequency: The target frequency: "A", "Q" or "M".

    Returns:
        The period, or None if the label is not a recognisable period.
    """
    pandas_frequency = FREQUENCY_TO_PANDAS[frequency]
    if isinstance(label, datetime | pd.Timestamp):
        return pd.Period(label, freq=pandas_frequency)
    if _is_numeric_year(label):
        label = str(int(label))
    if not isinstance(label, str):
        return None
    text = " ".join(label.strip().upper().split())
    if match := QUARTER_PATTERN.match(text):
        period = pd.Period(year=int(match[1]), quarter=int(match[2]), freq="Q-DEC")
    elif match := QUARTER_FIRST_PATTERN.match(text):
        period = pd.Period(year=int(match[2]), quarter=int(match[1]), freq="Q-DEC")
    elif match := YEAR_MONTH_PATTERN.match(text):
        period = _month_period(match[2], match[1])
    elif match := MONTH_YEAR_PATTERN.match(text) or MONTH_FULL_YEAR_PATTERN.match(text):
        period = _month_period(match[1], match[2])
    elif match := YEAR_PATTERN.match(text):
        period = pd.Period(year=int(match[1]), freq="Y-DEC")
    else:
        return None
    if period is None:
        return None
    return _convert_if_finer(period, pandas_frequency)


def _is_numeric_year(label: object) -> bool:
    """Checks whether a label is a number that reads as a plausible year.

    Args:
        label: The raw label.

    Returns:
        True for whole numbers from 1900 to 2100, such as 1993 or 1993.0.
    """
    if isinstance(label, bool) or not isinstance(label, int | float):
        return False
    return (
        float(label).is_integer()
        and EARLIEST_NUMERIC_YEAR <= label <= LATEST_NUMERIC_YEAR
    )


def is_three_month_range(label: object) -> bool:
    """Checks whether a label describes a rolling three-month window, e.g. "2026 MAY-JUL".

    Args:
        label: The raw label.

    Returns:
        True if the label is a three-month range.
    """
    return isinstance(label, str) and bool(
        THREE_MONTH_RANGE_PATTERN.match(" ".join(label.strip().upper().split()))
    )


def period_to_string(period: pd.Period) -> str:
    """Writes a period in the canonical string form used in the archive.

    Args:
        period: An annual, quarterly or monthly period.

    Returns:
        "2024", "2024Q1" or "2024-03".
    """
    return str(period)


def string_to_period(text: str) -> pd.Period:
    """Reads a canonical period string back into a pandas period.

    Args:
        text: "2024", "2024Q1" or "2024-03".

    Returns:
        The matching period.
    """
    return pd.Period(text)


def frequency_of(text: str) -> str:
    """Gives the frequency code of a canonical period string.

    Args:
        text: "2024", "2024Q1" or "2024-03".

    Returns:
        "A", "Q" or "M".
    """
    return PANDAS_TO_FREQUENCY[string_to_period(text).freqstr]


def _month_period(month_abbreviation: str, year_text: str) -> pd.Period | None:
    """Builds a monthly period from a month abbreviation and a two- or four-digit year.

    Args:
        month_abbreviation: For example "MAR".
        year_text: For example "24" or "2024".

    Returns:
        The monthly period, or None if the month is not recognised.
    """
    year_format = "%Y" if len(year_text) == 4 else "%y"
    try:
        timestamp = pd.to_datetime(
            f"{month_abbreviation} {year_text}", format=f"%b {year_format}"
        )
    except ValueError:
        return None
    return pd.Period(timestamp, freq="M")


def _convert_if_finer(period: pd.Period, pandas_frequency: str) -> pd.Period | None:
    """Converts a period to a coarser target frequency; rejects a coarser label.

    Args:
        period: The parsed period.
        pandas_frequency: The target pandas frequency string.

    Returns:
        The period at the target frequency, or None if the label was coarser
        than the target (for example a year where a month was expected).
    """
    if period.freqstr == pandas_frequency:
        return period
    order = list(FREQUENCY_TO_PANDAS.values())
    if order.index(period.freqstr) < order.index(pandas_frequency):
        return None
    return period.asfreq(pandas_frequency)
