"""Command-line entry point: `realtime build` and `realtime report`."""

import argparse
import sys

from loguru import logger

from realtime.benchmark import benchmark_report
from realtime.build import SOURCE_TYPES, build_archive
from realtime.cache import DownloadCache
from realtime.config import load_config
from realtime.report import coverage_report
from realtime.settings import ARCHIVE_PATH, BENCHMARK_PATH, DIAGNOSTICS_PATH, LOG_DIR
from realtime.store import read_archive, write_archive
from realtime.validate import growth_agreement_report, look_ahead_summary

LOG_ROTATION = "10 MB"
LOG_RETENTION_FILES = 10


def configure_logging(verbose: bool) -> None:
    """Sends logs to the terminal and to a rotating file in logs/.

    Args:
        verbose: If True, show debug messages in the terminal.
    """
    logger.remove()
    logger.add(
        sys.stderr,
        level="DEBUG" if verbose else "INFO",
        format="{time:HH:mm:ss} | {level} | {message}",
    )
    logger.add(
        LOG_DIR / "realtime.log",
        level="DEBUG",
        rotation=LOG_ROTATION,
        retention=LOG_RETENTION_FILES,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    )


def run_build(arguments: argparse.Namespace) -> None:
    """Builds the archive and writes the parquet file and diagnostics.

    Args:
        arguments: Parsed command-line arguments.
    """
    cache = DownloadCache(offline=arguments.offline)
    changes = build_archive(load_config(), cache, only=arguments.only)
    written = write_archive(changes, ARCHIVE_PATH)
    report = coverage_report(written)
    report.to_csv(DIAGNOSTICS_PATH, index=False)
    logger.info(f"Diagnostics written to {DIAGNOSTICS_PATH}")


def run_report(arguments: argparse.Namespace) -> None:
    """Prints the diagnostics report for the existing archive.

    Args:
        arguments: Parsed command-line arguments.
    """
    report = coverage_report(read_archive(ARCHIVE_PATH))
    print(report.to_string(index=False))


def run_validate(arguments: argparse.Namespace) -> None:
    """Prints the consistency checks for the existing archive.

    Args:
        arguments: Parsed command-line arguments.
    """
    archive = read_archive(ARCHIVE_PATH)
    early = look_ahead_summary(archive)
    print("Rows dated on or before the end of their own period (should be none):")
    print(early.to_string(index=False) if not early.empty else "  none")
    print()
    print("Growth triangles against growth worked out from levels:")
    print(growth_agreement_report(archive).to_string(index=False))


def run_benchmark(arguments: argparse.Namespace) -> None:
    """Compares the archive with the OECD revisions database and writes the results.

    Args:
        arguments: Parsed command-line arguments.
    """
    cache = DownloadCache(offline=arguments.offline)
    report = benchmark_report(read_archive(ARCHIVE_PATH), cache)
    report.to_csv(BENCHMARK_PATH, index=False)
    print(report.to_string(index=False))
    logger.info(f"Benchmark written to {BENCHMARK_PATH}")


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Reads the command line.

    Args:
        argv: Arguments to parse; defaults to sys.argv.

    Returns:
        The parsed arguments.
    """
    parser = argparse.ArgumentParser(
        prog="realtime", description="Build a real-time archive of ONS data."
    )
    parser.add_argument("--verbose", action="store_true", help="show debug messages")
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser(
        "build", help="download (or reuse cached) data and write the archive"
    )
    build.add_argument(
        "--offline", action="store_true", help="use only cached files; never download"
    )
    build.add_argument(
        "--only", nargs="+", choices=sorted(SOURCE_TYPES), help="run only these sources"
    )
    build.set_defaults(handler=run_build)
    report = commands.add_parser(
        "report", help="print coverage diagnostics for the archive"
    )
    report.set_defaults(handler=run_report)
    validate = commands.add_parser(
        "validate", help="check the archive for look-ahead and cross-source agreement"
    )
    validate.set_defaults(handler=run_validate)
    benchmark = commands.add_parser(
        "benchmark", help="compare the archive with the OECD revisions database"
    )
    benchmark.add_argument(
        "--offline", action="store_true", help="use only cached files; never download"
    )
    benchmark.set_defaults(handler=run_benchmark)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Runs the command line interface.

    Args:
        argv: Arguments to parse; defaults to sys.argv.
    """
    arguments = parse_arguments(argv)
    configure_logging(arguments.verbose)
    arguments.handler(arguments)


if __name__ == "__main__":
    main()
