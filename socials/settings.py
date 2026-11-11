"""Reads the settings for social media videos and stills from socials.toml."""

import tomllib
from dataclasses import dataclass, fields
from pathlib import Path

SETTINGS_PATH = Path(__file__).parent / "socials.toml"
OUTPUT_SECTION = "output"
VIDEO_SECTION = "video"
STILL_SECTION = "still"


@dataclass(frozen=True)
class OutputSettings:
    """Where videos and stills are written, and the footer they share.

    Attributes:
        directory: The folder the files are written to.
        footer: Text shown along the bottom of every video and still.
    """

    directory: Path
    footer: str


@dataclass(frozen=True)
class StillSettings:
    """The size of PNG stills, which show the final frame of each animation.

    Attributes:
        width_px: Image width in pixels.
        height_px: Image height in pixels. Must keep the 16:9 shape of the videos.
    """

    width_px: int
    height_px: int


@dataclass(frozen=True)
class VideoSettings:
    """How MP4 videos are recorded.

    Attributes:
        width_px: Video width in pixels.
        height_px: Video height in pixels.
        frames_per_second: Frames recorded for each second of animation.
        end_hold_seconds: How long the final frame stays on screen.
        quality: The x264 constant rate factor; lower is sharper and larger, 18 looks lossless.
    """

    width_px: int
    height_px: int
    frames_per_second: int
    end_hold_seconds: float
    quality: int


@dataclass(frozen=True)
class TapeStackSettings:
    """Settings for the tape stack video, the landing page animation.

    Attributes:
        series_id: The quarterly series to show.
        first_quarter: The earliest quarter to show.
        headline: The video's headline.
    """

    series_id: str
    first_quarter: str
    headline: str


@dataclass(frozen=True)
class EstimatePathSettings:
    """Settings for the video showing every estimate of one quarter.

    Attributes:
        series_id: The quarterly series to show.
        quarter: The quarter to follow, such as "1997Q1".
        headline: The headline.
        first_vintage: The earliest publication date to show.
        last_vintage: The latest publication date to show.
    """

    series_id: str
    quarter: str
    headline: str
    first_vintage: str | None = None
    last_vintage: str | None = None


@dataclass(frozen=True)
class RevisionHeatmapSettings:
    """Settings for the revision triangle video.

    Attributes:
        series_id: The quarterly series to show.
        headline: The headline.
        colour_limit: The revision, in percentage points, at which colour is at full strength.
        first_quarter: The earliest quarter to show.
        last_quarter: The latest quarter to show.
        first_vintage: The earliest publication date to show.
        last_vintage: The latest publication date to show.
    """

    series_id: str
    headline: str
    colour_limit: float
    first_quarter: str | None = None
    last_quarter: str | None = None
    first_vintage: str | None = None
    last_vintage: str | None = None


@dataclass(frozen=True)
class VintageOverlaySettings:
    """Settings for the video drawing every vintage on top of the others.

    Attributes:
        series_id: The quarterly series to show.
        headline: The headline.
        value_limit: The vertical axis runs from minus this value to plus this value.
        first_quarter: The earliest quarter to show.
        last_quarter: The latest quarter to show.
        first_vintage: The earliest publication date to show.
        last_vintage: The latest publication date to show.
    """

    series_id: str
    headline: str
    value_limit: float
    first_quarter: str | None = None
    last_quarter: str | None = None
    first_vintage: str | None = None
    last_vintage: str | None = None


@dataclass(frozen=True)
class TwoClocksSettings:
    """Settings for the video linking reference quarters to publication dates.

    Attributes:
        series_id: The quarterly series to show.
        headline: The headline.
        first_quarter: The earliest quarter to show.
        last_quarter: The latest quarter to show.
        first_vintage: The earliest publication date to show.
        last_vintage: The latest publication date to show.
    """

    series_id: str
    headline: str
    first_quarter: str | None = None
    last_quarter: str | None = None
    first_vintage: str | None = None
    last_vintage: str | None = None


@dataclass(frozen=True)
class ArchiveStatsSettings:
    """Settings for the video showing the archive's headline numbers.

    Attributes:
        headline: The headline.
        as_of: Count only what had been published by this date.
    """

    headline: str
    as_of: str | None = None


@dataclass(frozen=True)
class ReleaseCalendarSettings:
    """Settings for the video showing every publication date.

    Attributes:
        headline: The headline.
        highlight_above: Days with more values than this are drawn in amber.
        first_date: The earliest publication date to show.
        last_date: The latest publication date to show.
    """

    headline: str
    highlight_above: int
    first_date: str | None = None
    last_date: str | None = None


@dataclass(frozen=True)
class WeekdayRhythmSettings:
    """Settings for the video showing values published by day of the week.

    Attributes:
        headline: The headline.
        first_date: The earliest publication date to count.
        last_date: The latest publication date to count.
    """

    headline: str
    first_date: str | None = None
    last_date: str | None = None


@dataclass(frozen=True)
class ProvenanceSettings:
    """Settings for the video showing source file fingerprints.

    Attributes:
        headline: The headline.
        track_count: The number of rows of fingerprints.
        record_count: The number of archive rows shown among them.
        seed: Picks which rows are shown; the same seed always gives the same card.
        first_vintage: The earliest publication date to pick rows from.
        last_vintage: The latest publication date to pick rows from.
    """

    headline: str
    track_count: int
    record_count: int
    seed: int
    first_vintage: str | None = None
    last_vintage: str | None = None


@dataclass(frozen=True)
class TitleCardSettings:
    """Settings for the title video.

    Attributes:
        title: The large title text.
        subtitle: The line under the title.
        split_px: How far the cyan and magenta copies of the title sit from the white one.
    """

    title: str
    subtitle: str
    split_px: float


def read_settings(path: Path = SETTINGS_PATH) -> dict[str, dict]:
    """Reads the settings file.

    Args:
        path: The TOML file to read.

    Returns:
        One dictionary of settings per section.

    Raises:
        FileNotFoundError: If the file does not exist.
        tomllib.TOMLDecodeError: If the file is not valid TOML.
    """
    with path.open("rb") as handle:
        return tomllib.load(handle)


def output_settings(raw: dict[str, dict]) -> OutputSettings:
    """Builds the output settings from the [output] section.

    Args:
        raw: The whole settings file, as returned by read_settings.

    Returns:
        The output settings.
    """
    section = dict(raw[OUTPUT_SECTION])
    section["directory"] = Path(section["directory"])
    return build_settings(OutputSettings, section, OUTPUT_SECTION)


def build_settings[T](settings_class: type[T], section: dict, section_name: str) -> T:
    """Builds one settings object, naming the section if a setting is wrong or missing.

    Args:
        settings_class: The dataclass to build.
        section: The settings from one section of the file.
        section_name: The section's name, used in error messages.

    Returns:
        The settings object.

    Raises:
        ValueError: If the section has an unknown setting or is missing a required one.
    """
    known = {field.name for field in fields(settings_class)}
    unknown = sorted(set(section) - known)
    if unknown:
        raise ValueError(f"[{section_name}] has unknown settings: {', '.join(unknown)}")
    try:
        return settings_class(**section)
    except TypeError as error:
        raise ValueError(f"[{section_name}] is missing a setting: {error}") from error


def video_settings(raw: dict[str, dict]) -> VideoSettings:
    """Builds the video settings from the [video] section.

    Args:
        raw: The whole settings file, as returned by read_settings.

    Returns:
        The video settings.
    """
    return build_settings(VideoSettings, dict(raw[VIDEO_SECTION]), VIDEO_SECTION)


def still_settings(raw: dict[str, dict]) -> StillSettings:
    """Builds the still image settings from the [still] section.

    Args:
        raw: The whole settings file, as returned by read_settings.

    Returns:
        The still image settings.
    """
    return build_settings(StillSettings, dict(raw[STILL_SECTION]), STILL_SECTION)
