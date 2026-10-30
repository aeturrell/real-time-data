"""Reading series.toml: which series to collect, and from where."""

import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from realtime.settings import SERIES_CONFIG_PATH

DEFAULT_SHEET_PATTERN = "triangle"


@dataclass(frozen=True)
class SeriesSpec:
    """A series collected from the past versions of its ONS time-series pages.

    Attributes:
        series_id: The ONS CDID, for example "ABMI".
        short_name: Our short label.
        measure: Units and adjustment, for example "£m; CVM; SA".
        primary_dataset: The dataset whose page wins when pages disagree on a day.
        exclude_datasets: Datasets whose pages for this series should be ignored.
        unrevised_from: For series ONS never revises, the first reference period
            (for example "1997-01") from which today's values can stand in for
            first publications.
    """

    series_id: str
    short_name: str
    measure: str
    primary_dataset: str
    exclude_datasets: tuple[str, ...] = ()
    unrevised_from: str | None = None


@dataclass(frozen=True)
class TriangleSpec:
    """A series collected from an ONS revisions-triangle spreadsheet.

    Attributes:
        series_id: The ONS CDID where one exists, otherwise our own code.
        short_name: Our short label.
        title: A descriptive title.
        measure: Units and adjustment.
        frequency: "M" or "Q".
        basis: "period", or "3m_ending" for rolling three-month figures.
        dataset_path: The path of the ONS dataset page.
        file_pattern: Regular expression picking the file from the page's downloads.
        member_pattern: Regular expression picking the file inside a zip download.
        sheet_pattern: Regular expression picking the worksheet.
    """

    series_id: str
    short_name: str
    title: str
    measure: str
    frequency: str
    basis: str
    dataset_path: str
    file_pattern: str | None = None
    member_pattern: str | None = None
    sheet_pattern: str = DEFAULT_SHEET_PATTERN


@dataclass(frozen=True)
class RealtimeDbSpec:
    """A series collected from an ONS real-time database spreadsheet.

    Attributes:
        series_id: The ONS CDID.
        short_name: Our short label.
        title: A descriptive title.
        measure: Units and adjustment.
        dataset_path: The path of the ONS dataset page.
        sheet_pattern: Regular expression matching every worksheet that holds
            part of the database (they are read in workbook order).
        file_pattern: Regular expression picking the file from the page's downloads.
    """

    series_id: str
    short_name: str
    title: str
    measure: str
    dataset_path: str
    sheet_pattern: str
    file_pattern: str | None = None


@dataclass(frozen=True)
class ArchiveConfig:
    """Everything the build needs to know about what to collect.

    Attributes:
        series: Series collected from time-series page histories.
        triangles: Series collected from revisions triangles.
        realtime_dbs: Series collected from ONS real-time databases.
    """

    series: tuple[SeriesSpec, ...] = field(default_factory=tuple)
    triangles: tuple[TriangleSpec, ...] = field(default_factory=tuple)
    realtime_dbs: tuple[RealtimeDbSpec, ...] = field(default_factory=tuple)

    def short_names(self) -> dict[str, str]:
        """Maps every series ID to its short name.

        Returns:
            Series ID to short name; time-series specs take precedence.
        """
        from_databases = {spec.series_id: spec.short_name for spec in self.realtime_dbs}
        from_triangles = {spec.series_id: spec.short_name for spec in self.triangles}
        from_series = {spec.series_id: spec.short_name for spec in self.series}
        return from_databases | from_triangles | from_series

    def primary_datasets(self) -> dict[str, str]:
        """Maps each time-series ID to its primary dataset.

        Returns:
            Series ID to dataset ID.
        """
        return {spec.series_id: spec.primary_dataset for spec in self.series}


def load_config(path: Path = SERIES_CONFIG_PATH) -> ArchiveConfig:
    """Reads and checks the series configuration file.

    Args:
        path: Location of series.toml.

    Returns:
        The parsed configuration.

    Raises:
        ValueError: If a series ID appears twice in the same section.
    """
    with path.open("rb") as config_file:
        raw = tomllib.load(config_file)
    series = tuple(_series_spec(entry) for entry in raw.get("series", []))
    triangles = tuple(_triangle_spec(entry) for entry in raw.get("triangle", []))
    databases = tuple(_realtime_db_spec(entry) for entry in raw.get("realtime_db", []))
    _check_unique([spec.series_id for spec in series], "series")
    _check_unique([spec.series_id for spec in triangles], "triangle")
    _check_unique([spec.series_id for spec in databases], "realtime_db")
    return ArchiveConfig(series=series, triangles=triangles, realtime_dbs=databases)


def _series_spec(entry: dict) -> SeriesSpec:
    """Builds a SeriesSpec from one [[series]] table.

    Args:
        entry: The raw TOML table.

    Returns:
        The spec.
    """
    return SeriesSpec(
        series_id=entry["id"].upper(),
        short_name=entry["short_name"],
        measure=entry["measure"],
        primary_dataset=entry["primary_dataset"].upper(),
        exclude_datasets=tuple(
            dataset.upper() for dataset in entry.get("exclude_datasets", [])
        ),
        unrevised_from=entry.get("unrevised_from"),
    )


def _triangle_spec(entry: dict) -> TriangleSpec:
    """Builds a TriangleSpec from one [[triangle]] table.

    Args:
        entry: The raw TOML table.

    Returns:
        The spec.
    """
    return TriangleSpec(
        series_id=entry["id"].upper(),
        short_name=entry["short_name"],
        title=entry["title"],
        measure=entry["measure"],
        frequency=entry["frequency"],
        basis=entry.get("basis", "period"),
        dataset_path=entry["dataset_path"],
        file_pattern=entry.get("file_pattern"),
        member_pattern=entry.get("member_pattern"),
        sheet_pattern=entry.get("sheet_pattern", DEFAULT_SHEET_PATTERN),
    )


def _realtime_db_spec(entry: dict) -> RealtimeDbSpec:
    """Builds a RealtimeDbSpec from one [[realtime_db]] table.

    Args:
        entry: The raw TOML table.

    Returns:
        The spec.
    """
    return RealtimeDbSpec(
        series_id=entry["id"].upper(),
        short_name=entry["short_name"],
        title=entry["title"],
        measure=entry["measure"],
        dataset_path=entry["dataset_path"],
        sheet_pattern=entry["sheet_pattern"],
        file_pattern=entry.get("file_pattern"),
    )


def _check_unique(series_ids: list[str], section: str) -> None:
    """Checks that no series ID appears twice in a section.

    Args:
        series_ids: The IDs in the section.
        section: The section name, for the error message.

    Raises:
        ValueError: If there are duplicates.
    """
    duplicates = sorted(
        {series_id for series_id in series_ids if series_ids.count(series_id) > 1}
    )
    if duplicates:
        raise ValueError(f"Duplicate IDs in [[{section}]]: {duplicates}")
