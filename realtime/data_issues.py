"""Reading data_issues.toml: the log of errors found in published source files."""

import tomllib
from dataclasses import dataclass
from pathlib import Path

from realtime.settings import DATA_ISSUES_PATH


@dataclass(frozen=True)
class DataIssue:
    """An error found in a published source file.

    Attributes:
        id: A short, permanent code for the issue.
        publisher: The organisation that published the file.
        dataset: The dataset or survey affected.
        title: The issue in one line.
        logged: The date the issue was added to the log.
        location: Where the error is.
        problem: What is wrong.
        effect: What goes wrong if the values are used as published.
        handling: What the archive does about it.
        published: The date of the affected file, if known.
        correction: What the correct content is, if known.
        file_name: The name of the affected file, if it has one.
        source_page: The address of the publisher's page for the file, if known.
    """

    id: str
    publisher: str
    dataset: str
    title: str
    logged: str
    location: str
    problem: str
    effect: str
    handling: str
    published: str | None = None
    correction: str | None = None
    file_name: str | None = None
    source_page: str | None = None


def load_data_issues(path: Path = DATA_ISSUES_PATH) -> list[DataIssue]:
    """Reads the issue log, sorted by publisher and then newest file first.

    Args:
        path: The log's location.

    Returns:
        The issues.

    Raises:
        ValueError: If two issues share an id.
    """
    with path.open("rb") as issue_file:
        issues = [DataIssue(**entry) for entry in tomllib.load(issue_file)["issue"]]
    if len({issue.id for issue in issues}) != len(issues):
        raise ValueError(f"Duplicate issue ids in {path}")
    newest_first = sorted(issues, key=lambda issue: issue.published or "", reverse=True)
    return sorted(newest_first, key=lambda issue: issue.publisher)
