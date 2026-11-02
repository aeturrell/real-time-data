"""Copies the archive into the rendered site, so the two are always published together."""

import os
import shutil
from pathlib import Path

from loguru import logger

from realtime.settings import ARCHIVE_PATH

OUTPUT_DIRECTORY_VARIABLE = "QUARTO_PROJECT_OUTPUT_DIR"


def publish_archive(archive_path: Path, output_directory: Path) -> Path:
    """Copies the archive into the site's output folder.

    Args:
        archive_path: The parquet file the site's pages were built from.
        output_directory: The folder holding the rendered site.

    Returns:
        The path of the copied file.

    Raises:
        FileNotFoundError: If the archive has not been built.
        OSError: If the file cannot be copied.
    """
    if not archive_path.is_file():
        raise FileNotFoundError(
            f"No archive at {archive_path}. Run `uv run realtime build` first."
        )
    published_path = output_directory / archive_path.name
    shutil.copy2(archive_path, published_path)
    logger.info(f"Copied the archive to {published_path}")
    return published_path


def main() -> None:
    """Publishes the archive to the output folder that Quarto names."""
    publish_archive(ARCHIVE_PATH, Path(os.environ[OUTPUT_DIRECTORY_VARIABLE]))


if __name__ == "__main__":
    main()
