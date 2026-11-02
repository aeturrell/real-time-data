# real-time-data

## Introduction

This repo builds a real-time (vintage) archive of UK economic data, in the spirit of [ALFRED](https://alfred.stlouisfed.org/). For every series it records not just the latest numbers, but what was published on each date: the first estimate and every revision since. The figures currently come from two publishers: the Office for National Statistics (ONS) and the Bank of England.

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
uv run realtime validate         # check for look-ahead, late releases and mixed bases, and compare growth triangles with levels
uv run realtime benchmark        # compare with the OECD's independent real-time database
```

`validate` runs four checks:

- **Look-ahead:** any value dated on or before the end of the period it describes, which would mean a wrongly dated vintage. Surveys are left out, because they can properly report on a month before it ends.
- **Mixed bases:** any monthly series that mixes single months with rolling three-month windows.
- **Late first releases:** any period that first appears in the archive more than a month later than in one of the sources collected, which would mean a source was wrongly set aside. This needs every source's rows, so it runs during the build and is saved to `scratch/late_first_appearances.csv`.
- **Growth against levels:** each growth-rate triangle is compared with growth worked out from the matching level series at every vintage.

`benchmark` checks the archive against the OECD's short-term economic statistics revisions database, which has kept every monthly edition of its UK figures since February 1999 and was assembled independently of this archive. For each OECD edition, the latest 12 periods are compared with our figures from around the same date, and the share agreeing is written to `scratch/oecd_benchmark.csv`.

The first build downloads several thousand files, at a polite rate, so it takes a few hours. Past versions of published files and pages rarely change, so later builds mostly fetch only what is new.

Each build also writes `scratch/diagnostics.csv`, which shows for each series how many vintages it has, the dates they span, and how many are dated to the exact day. It also gives the usual (median) gap between vintages and the longest gap, with the date it ended. A gap more than three times the usual one is flagged as `unusual_gap`, because it usually means releases are missing.

## Terms

- **Vintage date**: the date a value was published.
- **Reference period**: the period a value describes, such as 2024 Q1 or March 2024. It is a proper period, not a timestamp.
- **Change-only storage**: a row is kept only when a value is first published or later revised. Any past vintage can be rebuilt from these rows.
- **CDID**: the four-character ONS code for a single time series, for example `ABMI` for GDP.

## The archive

One row per series, reference period and vintage date where the value was first published or changed:

| Column | Meaning |
|---|---|
| `series_id` | The publisher's code, such as the ONS CDID `ABMI`, or our own code (such as `IOS_MOM` or `DMP_SALES_REALISED_MEAN`) where there is none |
| `dataset_id` | The dataset or survey the value came from, such as `QNA`, `BICS`, `DMP`, `AGENTS` or `CCS` |
| `title`, `short_name`, `measure` | What the series is and its units |
| `frequency` | `A`, `Q`, `M` or `D` (daily, used for BICS waves, each stored against the day its survey closed) |
| `reference_period` | The period described |
| `reference_period_basis` | `period`, or `3m_ending` for rolling three-month labour market figures, which are stored against the month the window ends, or `survey` for survey results |
| `vintage_date` | When the value was published |
| `vintage_date_precision` | `day` (exact), `month` (only the publication month is known, so the last day of the month is used) or `upper_bound` (the latest date it could have been published) |
| `release_type` | `first` (the first estimate of a period), `revision` (a later change), or `history` (a period first seen in the archive more than a year after it ended, because no source reaches back to its first estimate; for example, GDP growth for 1955 to 1991 first appears in May 2015) |
| `value` | The published value |
| `source` | The organisation that published the value: `ONS` or `Bank of England` |
| `route` | How the value was collected, such as `series_json` or `boe_dmp` |
| `source_url`, `retrieved_at`, `content_sha256` | Where and when the value was obtained |

Vintage dates are never earlier than the true publication date, so the archive does not show data as known before it really was.

## Using the archive

```python
from realtime import as_of, convert_frequency, read_archive, revision_triangle, to_period_dtype

archive = read_archive()
gdp_in_june_2020 = as_of(archive, "2020-06-30").query("series_id == 'ABMI'")
triangle = revision_triangle(archive, "IHYQ")
archive["quarter"] = convert_frequency(archive["reference_period"], "Q")
```

To study first estimates only, keep rows where `release_type == "first"`; `history` rows are old figures, not releases.

`read_archive` turns `reference_period` into `pd.Period` objects. One table holds every frequency, so the column has object dtype; `to_period_dtype` gives a true `PeriodDtype` for a single-frequency subset.

## Recommended series for nowcasting

Several headline indicators are held in more than one form, for example GDP as a level (`ABMI`) and as a growth rate (`IHYQ`). For pseudo-real-time work, such as testing a nowcasting model on the data that was available at each date, the safest choice is the form that stays the same throughout the sample: the same ONS measure, the same units and the same rounding, from one source (or from sources ONS publishes identically).

In practice, this means preferring the published growth rates. Levels change units every few years, because ONS re-expresses chained volume measures in the prices of a new reference year. Old GDP vintages are even in 1954 or 1975 prices. So a level compared across vintages shows large jumps that are not revisions. Growth rates are not affected by this.

| Indicator | Recommended `series_id` | Vintages from | Notes |
|---|---|---|---|
| GDP, quarter on quarter | `IHYQ` | 1992 | Checked against the OECD: 99.9% agree |
| GDP, month on month | `ECYX` | 2018 | Monthly GDP was first published in July 2018 |
| Nominal GDP, quarter on quarter | `YBHA_QOQ` | 1992 | |
| GDP deflator, on a year ago | `YBGB_YOY` | 2006 | |
| Household consumption | `ABJR_QOQ` | 1996 | |
| Government consumption | `NMRY_QOQ` | 1996 | |
| Gross fixed capital formation | `NPQT_QOQ` | 1996 | The August 2002 vintage holds an ONS typing error (−5.5% for 2002 Q2) |
| Business investment | `KG7P` | 1998 | |
| Exports and imports (volume) | `IKBK_QOQ`, `IKBL_QOQ` | 1996 | |
| Production, month on month | `IOP_MOM` (`IOM_MOM` for manufacturing) | 1998 | |
| Services, month on month | `IOS_MOM` | 2000 | |
| Construction, month on month | `CON_MOM` | 2013 | |
| Retail sales volume, month on month | `RSI_MOM` | 1998 | Not checked against the OECD, whose UK retail series is a different measure |
| CPI inflation | `D7G7` | 1997 | Never revised, so earlier values are first publications; checked against the OECD: 100% agree |
| RPI | `CHAW` | 1997 | Never revised; checked against the OECD: 100% agree |
| Unemployment rate | `MGSX` | 1999 | Rolling three-month figures, stored against the month the window ends |
| Employment rate | `LF24` | 2010 | Rolling three-month figures, as above |
| Claimant count | `BCJD` | 1999 | |
| Household saving ratio | `NRJS` | 1998 | |

The same list, ready to copy:

```python
RECOMMENDED_SERIES = [
    "IHYQ", "ECYX", "YBHA_QOQ", "YBGB_YOY",
    "ABJR_QOQ", "NMRY_QOQ", "NPQT_QOQ", "KG7P", "IKBK_QOQ", "IKBL_QOQ",
    "IOP_MOM", "IOM_MOM", "IOS_MOM", "CON_MOM", "RSI_MOM",
    "D7G7", "CHAW",
    "MGSX", "LF24", "BCJD",
    "NRJS",
]
```

To rebuild the table's start years from the archive itself, with [DuckDB](https://duckdb.org/):

```bash
duckdb -c "SELECT series_id, any_value(title) AS title, year(min(vintage_date)) AS vintages_from, year(max(vintage_date)) AS vintages_to, count(*) AS rows FROM 'scratch/realtimedata.parquet' WHERE series_id IN ('IHYQ','ECYX','YBHA_QOQ','YBGB_YOY','ABJR_QOQ','NMRY_QOQ','NPQT_QOQ','KG7P','IKBK_QOQ','IKBL_QOQ','IOP_MOM','IOM_MOM','IOS_MOM','CON_MOM','RSI_MOM','D7G7','CHAW','MGSX','LF24','BCJD','NRJS') GROUP BY series_id ORDER BY vintages_from, series_id"
```

Things to be aware of:

- **Dating precision changes in late 2015.** Before then, most vintages are dated to the last day of their publication month (precision `month` or `upper_bound`); after it, to the exact day (`day`). To keep the information set defined the same way over the whole sample, move every vintage date to the end of its month:

  ```python
  import pandas as pd

  from realtime import as_of, read_archive

  archive = read_archive()
  archive["vintage_date"] = archive["vintage_date"] + pd.offsets.MonthEnd(0)
  known_end_2008 = as_of(archive, "2008-12-31")
  ```

  Month-end dates are the latest a value could have been published. So a model never sees data before it was released, although it may see some data a few weeks later than it really appeared.
- **Only from late 2015:** earnings growth (`KAC2`, `KAI9`), vacancies (`AP2Y`), public sector net borrowing (`J5II`), CPIH (`L55O`), core and services CPI (`DKO8`, `D7NN`), and the levels of employment and unemployment. Nothing published earlier is held for these.
- **GDP vintages from 1992 to 1998.** About 5% of growth rates in the `IHYQ` triangle differ, by up to 0.5 percentage points, from growth worked out from the `ABMI` real-time database. This is not yet explained, so treat this stretch with some caution.
- **When using levels** (for example `ABMI` back to 1961), compare values only within a single vintage. For instance, work out growth within each vintage and only then compare across vintages.
- **Missing releases.** Some releases are in no source collected. The labour market levels (`MGRZ`, `MGSC`, `YBUS`, `LF2S`) have no vintages between October 2023 and February 2024; the rates (`MGSX`, `LF24`) are filled from their triangles. `scratch/diagnostics.csv` flags gaps like this as `unusual_gap`.
- **Never-revised back-fills** (`D7G7` and `CHAW` before late 2015) are dated at the end of the month after the reference month. That is later than the true release, but never earlier.

## Sources

The series collected are listed in `series.toml`. They come from the following routes, plus the business surveys described in the next section.

- **Past versions of ONS time-series pages** (`[[series]]` entries). Every version of every ONS page for each series, from late 2015 onwards, with exact release dates.
- **Revisions triangles** (`[[triangle]]` entries). ONS revisions-triangle spreadsheets, which often go back to the 1990s but only give the publication month. Past versions of each triangle file are also read where they reach further back than the newest one.
- **Real-time databases** (`[[realtime_db]]` entries). ONS spreadsheets holding every published vintage of GDP (back to 1961), nominal GDP and the main expenditure components, with the publication month of each.
- **Never-revised series** (`[[series]]` entries with `unrevised_from`). For series ONS never revises, such as the Retail Prices Index, today's figures are the first publications. Each is dated at the end of the month after its reference period (the latest it could have been published), with precision `upper_bound`.

When several sources cover a series, a weaker source is used only in months where no stronger source has a vintage for that series. From strongest to weakest: exact-date page histories, then triangles and never-revised back-fills, then real-time databases. In practice, the weaker sources supply the years before the stronger ones begin, and also any later month the stronger ones miss (the page histories have no versions between May and September 2015, for example). Values that fill such a gap are rounded to the decimal places of the series' strongest source, so that differences in rounding do not show up as false revisions.

Rolling three-month labour market figures are marked with `basis = "3m_ending"` in `series.toml`, rather than being recognised from page labels. ONS labels them with their three-month window ("2015 AUG-OCT"), except on the first saved versions of October 2015, which give only the middle month.

## Business surveys

Four business surveys are treated in the same way as every other series, and are listed under `[[survey]]` in `series.toml`. Only the headline results are taken: for example, all businesses rather than each industry. Each survey's pages are found from the publisher's own listings, and the file links are read from those pages rather than guessed. The Bank of England's website is contacted at most once every three seconds, and its `robots.txt` allows these pages. Reuse terms for the Bank's data have not yet been reviewed.

| Survey | Publisher | `dataset_id` | Reference period | Vintage date |
|---|---|---|---|---|
| Business Insights and Conditions Survey | ONS | `BICS` | Day the wave's survey closed | Release date of each wave's file, or of its correction |
| Decision Maker Panel | Bank of England | `DMP` | Survey month | "Published on" date of each month's results page |
| Credit Conditions Survey | Bank of England | `CCS` | Survey quarter | "Published on" date of each quarter's results page |
| Agents' scores | Bank of England | `AGENTS` | Month of the score | Estimated from the Agents' report dates (see below) |

### Limits of these sources

**All four**

- These are opinions and reported experience from samples of firms, not official measurements. Net balances, shares and means are not comparable between surveys.
- A survey can report on a period before it ends, so the look-ahead check does not apply to them.
- Questions and layouts change. A series is only read where the published answer wording matches exactly, so some start later or stop earlier than the survey itself.

**BICS**

- Files with time-series worksheets begin at wave 30 (May 2021), but the answer wording used here first appears in December 2021 and mostly from May 2022. Earlier waves appear only when a later file repeats them, so their first date in the archive is late.
- Results for different groups of businesses are never joined. For example, demand was reported for businesses "not permanently stopped trading" until early 2024 and for those "currently trading" afterwards, with the later files adding history for the new group. Each group is its own series.
- Values are proportions between 0 and 1. Results suppressed by ONS (`[c]`) or not asked (`[x]`) are left out.
- 20 of the 160 editions carry the previous wave's release date, which falls before their own survey had closed. These use the next ONS release instead, with precision `upper_bound`, so they can be about two weeks late.
- Some older "Dates" worksheets contain mistyped dates (waves 20, 55 and 135). Every wave uses the date in the newest file.
- Editions released in the last 92 days are checked for corrections on each build. A correction to an older edition is not noticed unless its cached copy is removed.

**Decision Maker Panel**

- Results pages begin in October 2019, but the headline worksheets only appear from late 2021 or 2022, so earlier survey months first appear then, some as `history`.
- The Bank revises its whole history often as it refines its method, including changes in rounding and weighting. Many rows are therefore small revisions.
- December 2023 has only a "quarterly" data file, which is used in place of the monthly one.

**Credit Conditions Survey**

- Usable annex spreadsheets begin with 2015 Q4. Earlier results were published mostly as PDFs, and the 2015 Q3 file has a different layout, so quarters from 2007 to 2015 first appear on 15 January 2016 as `history`.
- Some annexes show values that later return to their earlier level, as in the 2019 Q2 and 2024 Q1 annexes. These look like slips in those files, but they are kept as published, so they appear as two revisions.

**Agents' scores**

- The Bank publishes one file with the whole history, so past versions cannot be seen. The scores are treated as never revised, with precision `upper_bound`.
- Each month is dated at the first Agents' report published after the 15th of that month. This follows the Bank's release pattern and is never meant to be early, but it can be up to about three months late when reports are quarterly and the file was updated between them.
- The scores first became public in January 2006, so all earlier months are dated then, and labelled `history`.
- Later dates are used for columns that cannot be shown to have been public earlier: June 2019 for the scores the Bank then introduced, and early 2024 for seven columns that first appear under their current names in a copy from that time. Those seven may simply have been renamed.
- Discontinued columns are no longer in the file and are not held.
- The Bank changed how it produces the scores in November 2021, from one decimal place to whole numbers, so the series is not fully consistent across that date.
- The scores are not strictly unrevised. Comparing archived copies with today's file found 12 changed values, all for January 2023. If the Bank corrects a value, the archive shows the new value with the original date, and a rebuild cannot show the old one.
- Archived copies of the file were used only to check these rules, not as data.

## Documentation

The `docs/` folder holds a [Quarto](https://quarto.org/) website: a landing page and a guide to the data. Its charts and figures are worked out from your local copy of the data, so run `uv run realtime build` and `uv run realtime benchmark` first. Then, with Quarto installed, run

```bash
uv run --group docs quarto render docs
```

The site is written to `docs/_site/`.

## Further development

See `PLAN.md` for the phased plan. Still to come: more UK publishers and series, archived ONS pages from the National Archives, the ONS API, and a review of the reuse terms for the Bank of England's data.
