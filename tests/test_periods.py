from datetime import datetime

import pandas as pd
import pytest

from realtime.periods import (
    frequency_of,
    is_three_month_range,
    parse_period_label,
    period_to_string,
)


@pytest.mark.parametrize(
    ("label", "frequency", "expected"),
    [
        ("2024 Q1", "Q", "2024Q1"),
        ("2024Q3", "Q", "2024Q3"),
        ("Q1 1955", "Q", "1955Q1"),
        ("2024 MAR", "M", "2024-03"),
        ("Mar-98", "M", "1998-03"),
        ("Feb 2010", "M", "2010-02"),
        ("2024", "A", "2024"),
        (datetime(1997, 9, 1), "Q", "1997Q3"),
        (datetime(2010, 6, 1), "M", "2010-06"),
        ("Jun-24", "Q", "2024Q2"),
        (1993.0, "A", "1993"),
        (1994, "A", "1994"),
    ],
)
def test_parse_period_label(label: object, frequency: str, expected: str) -> None:
    assert period_to_string(parse_period_label(label, frequency)) == expected


@pytest.mark.parametrize(
    ("label", "frequency"),
    [
        ("First Estimate", "M"),
        ("Month 2 estimate", "M"),
        ("2024", "M"),
        (None, "Q"),
        (3.5, "M"),
        (1993.0, "M"),
        (870.0, "A"),
    ],
)
def test_parse_period_label_rejects_non_periods(label: object, frequency: str) -> None:
    assert parse_period_label(label, frequency) is None


def test_three_month_range() -> None:
    assert is_three_month_range("2026 MAY-JUL")
    assert not is_three_month_range("2026 JUL")


@pytest.mark.parametrize(
    ("text", "expected"), [("2024", "A"), ("2024Q1", "Q"), ("2024-03", "M")]
)
def test_frequency_of(text: str, expected: str) -> None:
    assert frequency_of(text) == expected


def test_canonical_strings_round_trip() -> None:
    for text in ["2024", "2024Q1", "2024-03"]:
        assert period_to_string(pd.Period(text)) == text
