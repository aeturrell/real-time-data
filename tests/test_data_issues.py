"""Tests for the log of errors found in published source files."""

from pathlib import Path

import pytest

from realtime.data_issues import load_data_issues

ENTRY = """
[[issue]]
id = "{id}"
publisher = "{publisher}"
dataset = "A dataset"
title = "A title"
logged = "2026-11-02"
location = "A location"
problem = "A problem"
effect = "An effect"
handling = "A handling"
published = "{published}"
"""


def write_log(tmp_path: Path, *entries: str) -> Path:
    """Writes a small issue log.

    Args:
        tmp_path: The folder to write to.
        entries: The log's entries.

    Returns:
        The log's path.
    """
    path = tmp_path / "issues.toml"
    path.write_text("".join(entries))
    return path


def test_repo_log_loads_with_every_issue_described() -> None:
    assert all(issue.problem and issue.handling for issue in load_data_issues())


def test_issues_sort_by_publisher_then_newest_first(tmp_path: Path) -> None:
    path = write_log(
        tmp_path,
        ENTRY.format(id="a", publisher="ONS", published="2020-01-01"),
        ENTRY.format(id="b", publisher="BoE", published="2019-01-01"),
        ENTRY.format(id="c", publisher="ONS", published="2021-01-01"),
    )

    assert [issue.id for issue in load_data_issues(path)] == ["b", "c", "a"]


def test_duplicate_ids_are_rejected(tmp_path: Path) -> None:
    entry = ENTRY.format(id="a", publisher="ONS", published="2020-01-01")

    with pytest.raises(ValueError):
        load_data_issues(write_log(tmp_path, entry, entry))
