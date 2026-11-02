"""Reading series.toml: which series to collect, and from where."""

import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from realtime.schema import PERIOD_BASES, SURVEY_BASIS
from realtime.settings import SERIES_CONFIG_PATH

DEFAULT_SHEET_PATTERN = "triangle"
DEFAULT_BASIS = "period"
DEFAULT_PUBLIC_FROM = "2006-01"


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
        basis: "period", or "3m_ending" for rolling three-month figures, whose
            monthly values are stored against the month the window ends.
    """

    series_id: str
    short_name: str
    measure: str
    primary_dataset: str
    exclude_datasets: tuple[str, ...] = ()
    unrevised_from: str | None = None
    basis: str = DEFAULT_BASIS


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
        publication_lag_months: Months from a row's label to its publication, for
            triangles that label each row by its latest data month rather than by
            the month it was published.
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
    publication_lag_months: int = 0


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
class SurveySpec:
    """A series collected from the published files of a business survey.

    Attributes:
        series_id: Our own code, for example "BICS_TURNOVER_EXP_INCREASE".
        short_name: Our short label.
        title: A descriptive title.
        measure: Units and coverage.
        survey: Which survey: "BICS", "DMP", "AGENTS" or "CCS".
        sheet: Regular expression picking the worksheet.
        answer: Text the final level of the column's heading must equal.
        terms: Phrases the column's full heading must all contain.
        older_answer: The column's heading in earlier files, whose layout was simpler.
        trading_status: For BICS, the group of businesses the results describe.
        question: For the Credit Conditions Survey, the question's wording.
        horizon: For the Credit Conditions Survey, "Past three months" or
            "Next three months".
        public_from: For Agents' scores, the month from which the column is known
            to have been public, for example "2019-06".
        basis: How the reference period relates to the survey.
    """

    series_id: str
    short_name: str
    title: str
    measure: str
    survey: str
    sheet: str
    answer: str | None = None
    terms: tuple[str, ...] = ()
    older_answer: str | None = None
    trading_status: str | None = None
    question: str | None = None
    horizon: str | None = None
    public_from: str = DEFAULT_PUBLIC_FROM
    basis: str = SURVEY_BASIS


@dataclass(frozen=True)
class ArchiveConfig:
    """Everything the build needs to know about what to collect.

    Attributes:
        series: Series collected from time-series page histories.
        triangles: Series collected from revisions triangles.
        realtime_dbs: Series collected from ONS real-time databases.
        surveys: Series collected from the published files of business surveys.
    """

    series: tuple[SeriesSpec, ...] = field(default_factory=tuple)
    triangles: tuple[TriangleSpec, ...] = field(default_factory=tuple)
    realtime_dbs: tuple[RealtimeDbSpec, ...] = field(default_factory=tuple)
    surveys: tuple[SurveySpec, ...] = field(default_factory=tuple)

    def short_names(self) -> dict[str, str]:
        """Maps every series ID to its short name.

        Returns:
            Series ID to short name; time-series specs take precedence.
        """
        from_surveys = {spec.series_id: spec.short_name for spec in self.surveys}
        from_databases = {spec.series_id: spec.short_name for spec in self.realtime_dbs}
        from_triangles = {spec.series_id: spec.short_name for spec in self.triangles}
        from_series = {spec.series_id: spec.short_name for spec in self.series}
        return from_surveys | from_databases | from_triangles | from_series

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
    surveys = tuple(_survey_spec(entry) for entry in raw.get("survey", []))
    _check_unique([spec.series_id for spec in series], "series")
    _check_unique([spec.series_id for spec in triangles], "triangle")
    _check_unique([spec.series_id for spec in databases], "realtime_db")
    _check_unique([spec.series_id for spec in surveys], "survey")
    return ArchiveConfig(
        series=series, triangles=triangles, realtime_dbs=databases, surveys=surveys
    )


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
        basis=_basis(entry),
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
        basis=_basis(entry),
        dataset_path=entry["dataset_path"],
        file_pattern=entry.get("file_pattern"),
        member_pattern=entry.get("member_pattern"),
        sheet_pattern=entry.get("sheet_pattern", DEFAULT_SHEET_PATTERN),
        publication_lag_months=entry.get("publication_lag_months", 0),
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


def _survey_spec(entry: dict) -> SurveySpec:
    """Builds a SurveySpec from one [[survey]] table.

    Args:
        entry: The raw TOML table.

    Returns:
        The spec.
    """
    return SurveySpec(
        series_id=entry["id"].upper(),
        short_name=entry["short_name"],
        title=entry["title"],
        measure=entry["measure"],
        survey=entry["survey"],
        sheet=entry["sheet"],
        answer=entry.get("answer"),
        terms=tuple(entry.get("terms", [])),
        older_answer=entry.get("older_answer"),
        trading_status=entry.get("trading_status"),
        question=entry.get("question"),
        horizon=entry.get("horizon"),
        public_from=entry.get("public_from", DEFAULT_PUBLIC_FROM),
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


def _basis(entry: dict) -> str:
    """Reads and checks an entry's reference-period basis.

    Args:
        entry: The raw TOML table.

    Returns:
        The basis, "period" if none is given.

    Raises:
        ValueError: If the basis is not one the archive allows.
    """
    basis = entry.get("basis", DEFAULT_BASIS)
    if basis not in PERIOD_BASES:
        raise ValueError(f"{entry['id']}: unknown basis {basis!r}")
    return basis
