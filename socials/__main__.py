"""Records social media videos (MP4) and stills (PNG) from the archive.

uv run --group socials python -m socials mp4                  # every video
uv run --group socials python -m socials png                  # every still
uv run --group socials python -m socials mp4 tape_stack       # only the named ones
"""

import argparse
import sys
import tomllib
from collections.abc import Callable, Iterable
from pathlib import Path

import pandas as pd
from loguru import logger
from playwright.sync_api import Browser, sync_playwright

from realtime import read_archive
from realtime.settings import ARCHIVE_PATH
from socials.settings import (
    SETTINGS_PATH,
    OutputSettings,
    build_settings,
    output_settings,
    read_settings,
    still_settings,
    video_settings,
)
from socials.video import record_still, record_video
from socials.video_data import VIDEOS, Scene

FORMATS = ("mp4", "png")


def parse_arguments(arguments: list[str] | None) -> argparse.Namespace:
    """Reads the command-line arguments.

    Args:
        arguments: The arguments, or None to read them from the command line.

    Returns:
        The parsed arguments.
    """
    parser = argparse.ArgumentParser(
        prog="python -m socials",
        description="Record social media videos and stills from the archive.",
    )
    parser.add_argument(
        "format", choices=FORMATS, help="mp4 for videos, png for still final frames."
    )
    parser.add_argument(
        "names",
        nargs="*",
        help=f"Animations to make, from: {', '.join(VIDEOS)}. Leave out to make them all.",
    )
    parser.add_argument(
        "--settings",
        type=Path,
        default=SETTINGS_PATH,
        help="The settings file to read.",
    )
    parser.add_argument(
        "--archive", type=Path, default=ARCHIVE_PATH, help="The archive to draw from."
    )
    options = parser.parse_args(arguments)
    unknown = [name for name in options.names if name not in VIDEOS]
    if unknown:
        parser.error(
            f"unknown names: {', '.join(unknown)}. Choose from: {', '.join(VIDEOS)}"
        )
    return options


def build_scene(
    name: str, archive: pd.DataFrame, settings: dict[str, dict], footer: str
) -> Scene:
    """Builds one animation's data and text from its section of the settings file.

    Args:
        name: The animation, which is also its section name.
        archive: The change-only archive.
        settings: The whole settings file.
        footer: The footer text.

    Returns:
        The scene.

    Raises:
        ValueError: If a setting is unknown or missing, or leaves nothing to draw.
    """
    settings_class, scene_builder = VIDEOS[name]
    section = build_settings(settings_class, settings.get(name, {}), name)
    return scene_builder(archive, section, footer)


def make_each(names: Iterable[str], make_one: Callable[[str], Path]) -> int:
    """Makes each named output, logging and counting failures rather than stopping.

    Args:
        names: The animations to make.
        make_one: Makes one output and returns its path.

    Returns:
        The number that failed.
    """
    failures = 0
    for name in names:
        try:
            path = make_one(name)
        except (ValueError, OSError, RuntimeError) as error:
            logger.error(f"Could not make {name}: {error}")
            failures += 1
            continue
        logger.info(f"Wrote {path}")
    return failures


def output_maker(
    file_format: str,
    browser: Browser,
    archive: pd.DataFrame,
    settings: dict[str, dict],
    output: OutputSettings,
) -> Callable[[str], Path]:
    """Gives the function that makes one video or still.

    Args:
        file_format: "mp4" or "png".
        browser: A running headless browser.
        archive: The change-only archive.
        settings: The whole settings file.
        output: The output settings.

    Returns:
        A function that takes an animation's name and returns the file it wrote.

    Raises:
        KeyError: If the [video] or [still] section is missing.
        ValueError: If the [video] or [still] section is wrong.
    """
    if file_format == "mp4":
        video = video_settings(settings)
        return lambda name: record_video(
            browser,
            name,
            build_scene(name, archive, settings, output.footer),
            video,
            output.directory / f"{name}.mp4",
        )
    still = still_settings(settings)
    return lambda name: record_still(
        browser,
        name,
        build_scene(name, archive, settings, output.footer),
        still,
        output.directory / f"{name}.png",
    )


def main(arguments: list[str] | None = None) -> int:
    """Makes the chosen videos or stills, carrying on past any that fail.

    Args:
        arguments: The command-line arguments, or None to read them from the command line.

    Returns:
        0 if everything was made, otherwise 1.
    """
    options = parse_arguments(arguments)
    try:
        settings = read_settings(options.settings)
        output = output_settings(settings)
        archive = read_archive(options.archive)
    except (OSError, tomllib.TOMLDecodeError, KeyError, ValueError) as error:
        logger.error(f"Could not start: {error}")
        return 1

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        try:
            make_one = output_maker(options.format, browser, archive, settings, output)
        except (KeyError, ValueError) as error:
            logger.error(f"Could not start: {error}")
            return 1
        failures = make_each(options.names or list(VIDEOS), make_one)
        browser.close()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
