"""The columns of the archive, their types, and checks that a table follows them."""

import pandas as pd

KEY_COLUMNS = ["series_id", "reference_period", "vintage_date"]

CATEGORY_COLUMNS = [
    "series_id",
    "dataset_id",
    "title",
    "short_name",
    "measure",
    "frequency",
    "reference_period",
    "reference_period_basis",
    "vintage_date_precision",
    "source",
    "source_url",
    "content_sha256",
]

COLUMNS = [
    "series_id",
    "dataset_id",
    "title",
    "short_name",
    "measure",
    "frequency",
    "reference_period",
    "reference_period_basis",
    "vintage_date",
    "vintage_date_precision",
    "value",
    "source",
    "source_url",
    "retrieved_at",
    "content_sha256",
]

FREQUENCIES = frozenset({"A", "Q", "M"})
PERIOD_BASES = frozenset({"period", "3m_ending"})
VINTAGE_PRECISIONS = frozenset({"day", "month", "upper_bound"})


class SchemaError(ValueError):
    """Raised when a table does not follow the archive's schema."""


def conform(frame: pd.DataFrame) -> pd.DataFrame:
    """Puts a table's columns in the standard order and gives them the standard types.

    Reference periods stay as canonical strings (see realtime.periods); use
    realtime.store.read_archive to get pd.Period objects.

    Args:
        frame: A table with at least the archive's columns.

    Returns:
        A new table with exactly the archive's columns and types.

    Raises:
        SchemaError: If any column is missing.
    """
    missing = [column for column in COLUMNS if column not in frame.columns]
    if missing:
        raise SchemaError(f"Missing columns: {missing}")
    conformed = frame[COLUMNS].copy()
    for column in CATEGORY_COLUMNS:
        conformed[column] = conformed[column].astype("string").astype("category")
    conformed["vintage_date"] = pd.to_datetime(conformed["vintage_date"]).astype(
        "datetime64[ns]"
    )
    conformed["retrieved_at"] = pd.to_datetime(conformed["retrieved_at"], utc=True)
    conformed["value"] = pd.to_numeric(conformed["value"]).astype("float64")
    return conformed


def validate(frame: pd.DataFrame) -> None:
    """Checks a conformed table for missing keys, duplicate keys and unknown codes.

    Args:
        frame: A table produced by conform.

    Raises:
        SchemaError: If any check fails.
    """
    if frame[KEY_COLUMNS].isna().any().any():
        raise SchemaError("Key columns contain missing values")
    duplicated = frame.duplicated(subset=KEY_COLUMNS)
    if duplicated.any():
        examples = frame.loc[duplicated, KEY_COLUMNS].head().to_dict("records")
        raise SchemaError(f"{duplicated.sum()} duplicate keys, for example {examples}")
    _check_allowed(frame, "frequency", FREQUENCIES)
    _check_allowed(frame, "reference_period_basis", PERIOD_BASES)
    _check_allowed(frame, "vintage_date_precision", VINTAGE_PRECISIONS)


def _check_allowed(frame: pd.DataFrame, column: str, allowed: frozenset[str]) -> None:
    """Checks that a column only contains allowed codes.

    Args:
        frame: The table to check.
        column: The column name.
        allowed: The permitted values.

    Raises:
        SchemaError: If an unexpected value is present.
    """
    unexpected = set(frame[column].dropna().unique()) - allowed
    if unexpected:
        raise SchemaError(f"Unexpected values in {column}: {sorted(unexpected)}")
