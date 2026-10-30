# real-time-data

## Introduction

This repo builds a real-time (vintage) archive of UK macroeconomic data from the Office for National Statistics (ONS), in the spirit of [ALFRED](https://alfred.stlouisfed.org/). For every series it records not just the latest numbers, but what was published on each date: the first estimate and every revision since.

The result is a single tidy parquet file, `scratch/realtimedata.parquet`, built entirely from code. Every raw file downloaded is kept in `scratch/cache/` with a record of when it was fetched, so the archive can be rebuilt exactly, and new releases can be added at any time.

## Installing

The Python environment is managed by [uv](https://docs.astral.sh/uv/). To install it, run

```bash
uv sync
```

## Running

```bash
uv run realtime build            # download anything new and rebuild the archive
uv run realtime build --offline  # rebuild from cached files only, without downloading
uv run realtime build --only triangle
uv run realtime report           # print coverage diagnostics for each series
```

The first build downloads several thousand files, at a polite rate, so it takes a few hours. Past versions of ONS pages never change, so later builds only fetch what is new.

Each build also writes `scratch/diagnostics.csv`, which shows for each series how many vintages it has, the dates they span, how many are dated to the exact day, and any unusually long gaps.

## Terms

- **Vintage date**: the date a value was published by ONS.
- **Reference period**: the period a value describes, such as 2024 Q1 or March 2024. It is a proper period, not a timestamp.
- **Change-only storage**: a row is kept only when a value is first published or later revised. Any past vintage can be rebuilt from these rows.
- **CDID**: the four-character ONS code for a single time series, for example `ABMI` for GDP.

## The archive

One row per series, reference period and vintage date where the value was first published or changed:

| Column | Meaning |
|---|---|
| `series_id` | The ONS CDID, or our own code (such as `IOS_MOM`) where there is none |
| `dataset_id` | The ONS dataset the value came from, such as `QNA` |
| `title`, `short_name`, `measure` | What the series is and its units |
| `frequency` | `A`, `Q` or `M` |
| `reference_period` | The period described |
| `reference_period_basis` | `period`, or `3m_ending` for rolling three-month labour market figures, which are stored against the month the window ends |
| `vintage_date` | When ONS published the value |
| `vintage_date_precision` | `day` (exact), `month` (only the publication month is known, so the last day of the month is used) or `upper_bound` (the latest date it could have been published) |
| `value` | The published value |
| `source`, `source_url`, `retrieved_at`, `content_sha256` | Where and when the value was obtained |

Vintage dates are never earlier than the true publication date, so the archive does not show data as known before it really was.

## Using the archive

```python
from realtime import as_of, convert_frequency, read_archive, revision_triangle, to_period_dtype

archive = read_archive()
gdp_in_june_2020 = as_of(archive, "2020-06-30").query("series_id == 'ABMI'")
triangle = revision_triangle(archive, "IHYQ")
archive["quarter"] = convert_frequency(archive["reference_period"], "Q")
```

`read_archive` turns `reference_period` into `pd.Period` objects. One table holds every frequency, so the column has object dtype; `to_period_dtype` gives a true `PeriodDtype` for a single-frequency subset.

## Sources

The series collected are listed in `series.toml`.

- **Past versions of ONS time-series pages** (`[[series]]` entries). Every version of every ONS page for each series, from late 2015 onwards, with exact release dates.
- **Revisions triangles** (`[[triangle]]` entries). ONS revisions-triangle spreadsheets, which often go back to the 1990s but only give the publication month. Past versions of each triangle file are also read where they reach further back than the newest one.
- **Real-time databases** (`[[realtime_db]]` entries). ONS spreadsheets holding every published vintage of GDP (back to 1961), nominal GDP and the main expenditure components, with the publication month of each.
- **Never-revised series** (`[[series]]` entries with `unrevised_from`). For series ONS never revises, such as the Retail Prices Index, today's figures are the first publications. Each is dated at the end of the month after its reference period (the latest it could have been published), with precision `upper_bound`.

When several sources cover a series, each weaker one is used only for the period before every stronger one begins. From strongest to weakest: exact-date page histories, then triangles and never-revised back-fills, then real-time databases.

## Further development

See `PLAN.md` for the phased plan. Still to come: archived ONS pages from the National Archives, and the ONS API.
