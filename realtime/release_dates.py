"""Working out the vintage date (the day ONS published it) of each past version of a page.

Each ONS page lists its past versions, and each entry's "updateDate" is the moment
that version was replaced. So the replacement time of version N-1 is the moment
version N went live: that is its vintage date. The current page went live when
the last past version was replaced.

Versions made before the current ONS website launched (25 February 2016) were
created when content was migrated from the old site, so their dates are not
reliable. For those, a version's own "releaseDate" is used if it is later than
the previous vintage and no later than the version was replaced. Otherwise the
vintage date is set to the latest date the content could have been published (an
upper bound), so that the archive never shows data as known before it really was.

Occasionally a release overwrites a page without archiving the version before it,
so the inferred date is too early. This shows up as a version dated on or before
the end of the latest period it holds, which is impossible. Such a version is
moved to the earliest date that is possible ("releaseDate", which records the
latest release that touched the version, or else the time it was replaced) and
marked as an upper bound.
"""

from dataclasses import dataclass

import pandas as pd
from loguru import logger

from realtime.settings import LONDON_TIMEZONE

SITE_LAUNCH_DATE = pd.Timestamp("2016-02-25")


@dataclass(frozen=True)
class PageVersion:
    """One version of an ONS page, with the dates the website gives for it.

    Attributes:
        label: "v1", "v2", ... or "current".
        release_date: The "releaseDate" in the version's own description.
        replaced_at: When this version was replaced; None for the current page.
        latest_period_end: The last day of the latest period the version holds.
    """

    label: str
    release_date: pd.Timestamp | None
    replaced_at: pd.Timestamp | None
    latest_period_end: pd.Timestamp | None = None


@dataclass(frozen=True)
class Vintage:
    """The vintage date given to a page version.

    Attributes:
        label: The version label.
        vintage_date: The publication date (midnight, London calendar day).
        precision: "day" if known exactly, "upper_bound" if the latest possible date.
    """

    label: str
    vintage_date: pd.Timestamp
    precision: str


def london_date(timestamp: str | pd.Timestamp | None) -> pd.Timestamp | None:
    """Converts an ONS timestamp (UTC) to the calendar day in London.

    Args:
        timestamp: For example "2015-09-29T23:00:00.000Z".

    Returns:
        Midnight on the London calendar day, without a time zone, or None.
    """
    if timestamp is None or timestamp == "":
        return None
    moment = pd.Timestamp(timestamp)
    if moment.tzinfo is None:
        moment = moment.tz_localize("UTC")
    return moment.tz_convert(LONDON_TIMEZONE).tz_localize(None).normalize()


def assign_vintages(versions: list[PageVersion]) -> list[Vintage]:
    """Gives every version of a page a vintage date, in version order.

    Args:
        versions: Past versions in order (v1 first), followed by the current page.

    Returns:
        One vintage per version, in the same order.
    """
    vintages: list[Vintage] = []
    for index, version in enumerate(versions):
        went_live = versions[index - 1].replaced_at if index > 0 else None
        previous = vintages[-1].vintage_date if vintages else None
        vintage = _vintage_for(version, went_live, previous)
        vintages.append(_no_earlier_than_content(vintage, version))
    return vintages


def _vintage_for(
    version: PageVersion,
    went_live: pd.Timestamp | None,
    previous: pd.Timestamp | None,
) -> Vintage:
    """Chooses the vintage date for one version from the dates the website gives.

    Args:
        version: The version being dated.
        went_live: When the previous version was replaced, if there is one.
        previous: The vintage date given to the previous version, if any.

    Returns:
        The vintage for this version.
    """
    if went_live is not None and went_live >= SITE_LAUNCH_DATE:
        if previous is None or went_live >= previous:
            return Vintage(version.label, went_live, "day")
    if version.release_date is not None and _is_after(version.release_date, previous):
        if version.replaced_at is None or version.release_date <= version.replaced_at:
            return Vintage(version.label, version.release_date, "day")
    return Vintage(
        version.label, _latest_possible_date(version, previous), "upper_bound"
    )


def _no_earlier_than_content(vintage: Vintage, version: PageVersion) -> Vintage:
    """Moves a vintage later if it comes before the data it holds could exist.

    Args:
        vintage: The vintage chosen from the website's dates.
        version: The version being dated.

    Returns:
        The vintage unchanged if possible; otherwise the earliest possible date
        after the latest period ends, as an upper bound.
    """
    period_end = version.latest_period_end
    if period_end is None or vintage.vintage_date > period_end:
        return vintage
    candidates = [
        date
        for date in (version.release_date, version.replaced_at)
        if date is not None and date > period_end
    ]
    if not candidates:
        logger.warning(
            f"{version.label} holds data to {period_end.date()} but no date after that"
        )
        return vintage
    logger.debug(
        f"{version.label}: dated {vintage.vintage_date.date()} but holds data to "
        f"{period_end.date()}; moved to {candidates[0].date()}"
    )
    return Vintage(version.label, candidates[0], "upper_bound")


def _latest_possible_date(
    version: PageVersion, previous: pd.Timestamp | None
) -> pd.Timestamp:
    """Gives the latest date by which a version's content must have been published.

    Args:
        version: The version being dated.
        previous: The vintage date given to the previous version, if any.

    Returns:
        The earlier of the website launch date and the date the version was
        replaced, but never earlier than the previous vintage.
    """
    candidates = [
        date for date in (version.replaced_at, SITE_LAUNCH_DATE) if date is not None
    ]
    latest = min(candidates)
    if previous is not None and latest < previous:
        return previous
    return latest


def _is_after(date: pd.Timestamp, previous: pd.Timestamp | None) -> bool:
    """Checks that a date comes strictly after the previous vintage date.

    Args:
        date: The candidate date.
        previous: The previous vintage date, if any.

    Returns:
        True if there is no previous vintage or the date is later than it.
    """
    return previous is None or date > previous
