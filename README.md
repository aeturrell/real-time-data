# UK Real Time Data

[![DOI](https://zenodo.org/badge/427734171.svg)](https://doi.org/10.5281/zenodo.23900406)

## Introduction

This repo provides a real-time, vintage-aware archive of UK economic data, in the spirit of [ALFRED](https://alfred.stlouisfed.org/). For every series, it records what was published on what date. The figures currently come from two publishers: the Office for National Statistics (ONS) and the Bank of England.

The result is a single tidy parquet file, `realtimedata.parquet`.

> [!WARNING]
> This project has been substantially vibe coded. Use with caution.

## Using the archive

Please see the [docs](https://aeturrell.github.io/real-time-data/).

## Development

Every raw file downloaded is kept in `scratch/cache/` with a record of when it was fetched, so the archive can be rebuilt exactly, and new releases can be added at any time.

### Installing

The Python environment is managed by [uv](https://docs.astral.sh/uv/). To install it, run

```bash
uv sync
```

### Running

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

The first build downloads several thousand files, at a capped rate, so it takes a few hours. Past versions of published files and pages rarely change, so later builds mostly fetch only what is new.

Each build also writes `scratch/diagnostics.csv`, which shows for each series how many vintages it has, the dates they span, and how many are dated to the exact day. It also gives the usual (median) gap between vintages and the longest gap, with the date it ended. A gap more than three times the usual one is flagged as `unusual_gap`, because it usually means releases are missing.

### Social media videos and stills

`socials/` makes MP4 videos and PNG stills for social media from the archive, in the same style as the docs site. All settings are in `socials/socials.toml`.

```bash
uv run --group socials playwright install chromium     # once
uv run --group socials python -m socials mp4           # every video
uv run --group socials python -m socials png           # every still (final frame)
```

See [`socials/README.md`](socials/README.md) for what each animation shows, every setting, and how the recording works.

### Landing page animation

The animation at the top of the docs landing page is drawn from the archive each time the docs are rendered. `docs/_hero.py` packs every vintage of GDP growth into the page, and `docs/assets/tape_hero.js` draws it, so the animation picks up new vintages whenever the site is rebuilt.

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



## Sources

The series collected are listed in `series.toml`. They come from the following routes, plus the business surveys described in the next section.

- **Past versions of ONS time-series pages** (`[[series]]` entries). Every version of every ONS page for each series, from late 2015 onwards, with exact release dates.
- **Revisions triangles** (`[[triangle]]` entries). ONS revisions-triangle spreadsheets, which often go back to the 1990s but only give the publication month. Past versions of each triangle file are also read where they reach further back than the newest one.
- **Real-time databases** (`[[realtime_db]]` entries). ONS spreadsheets holding every published vintage of GDP (back to 1961), nominal GDP and the main expenditure components, with the publication month of each.
- **Never-revised series** (`[[series]]` entries with `unrevised_from`). For series ONS never revises, such as the Retail Prices Index, today's figures are the first publications. Each is dated at the end of the month after its reference period (the latest it could have been published), with precision `upper_bound`.

When several sources cover a series, a weaker source is used only in months where no stronger source has a vintage for that series. From strongest to weakest: exact-date page histories, then triangles and never-revised back-fills, then real-time databases. In practice, the weaker sources supply the years before the stronger ones begin, and also any later month the stronger ones miss (the page histories have no versions between May and September 2015, for example). Values that fill such a gap are rounded to the decimal places of the series' strongest source, so that differences in rounding do not show up as false revisions.

Rolling three-month labour market figures are marked with `basis = "3m_ending"` in `series.toml`, rather than being recognised from page labels. ONS labels them with their three-month window ("2015 AUG-OCT"), except on the first saved versions of October 2015, which give only the middle month.

## Business surveys

Four business surveys are treated in the same way as every other series, and are listed under `[[survey]]` in `series.toml`. Only the headline results are taken: for example, all businesses rather than each industry. Each survey's pages are found from the publisher's own listings, and the file links are read from those pages rather than guessed. The Bank of England's website is contacted at most once every three seconds, and its `robots.txt` allows these pages.

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
- The row of years above the quarters has slips in some annexes: a mistyped year in the January 2016 corporate sheet, a misplaced year in all three sheets of the 2019 Q2 annex and a missing year in the 2024 Q1 corporate sheet. So the first year written is taken as the start, and the year then moves on each time the quarter labels start again.

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

Errors found in published files are logged in `data_issues.toml`. Add each new one there, and the site's data issues page will list it.

The `docs/` folder holds a [Quarto](https://quarto.org/) website: a landing page and a guide to the data. Its charts and figures are worked out from `realtimedata.parquet` and files already in the repository. With Quarto installed, run

```bash
uv run --group docs quarto render docs
```

The site is written to `docs/_site/`.

## Releasing the site

The docs need two generated files, both kept in the repository so that GitHub Actions can build the site without rebuilding the data: `realtimedata.parquet` and `docs/oecd_gdp_growth.csv`, which holds only the recent OECD GDP growth rates for the guide's comparison chart. A full build takes hours and uses a large download cache, so GitHub Actions never runs one.

The guide's table of OECD comparisons and its count of late first appearances are fixed results written into `docs/guide.qmd`, with the date they were last checked. After a data update, run `uv run realtime benchmark`, which also refreshes `docs/oecd_gdp_growth.csv`, and `uv run realtime validate`, compare them with what the guide says, and update `OECD_COMPARISON`, `OUTSIDE_CHECKS_DATE` and the text if needed. The late first appearances are in `scratch/late_first_appearances.csv`.

To release a new version:

1. Update the data:

   ```bash
   uv run realtime build
   uv run realtime benchmark
   ```

2. Raise the version number with this command, not by editing `pyproject.toml` by hand. It also updates `uv.lock`, and CI and the deployment both fail if the two files disagree:

   ```bash
   uv version --bump minor
   ```

3. Merge these changes into `main` through a pull request.

When a merge into `main` changes the version in `pyproject.toml`, the `Deploy docs` workflow builds the site and publishes it to GitHub Pages. Other merges into `main` publish nothing. You can also run the workflow by hand from the repository's Actions tab, which publishes the branch you choose.

The same merge also starts the `Release` workflow, which tags the merge as `v` plus the version number (for example, `v0.2.0`) and creates a GitHub release. GitHub writes the release notes from the pull requests merged since the previous release, so clear pull request titles make clear notes. A version that already has a release is left alone, so changing another part of `pyproject.toml` creates nothing. If this workflow fails, or the current version has no release yet, open `Release` in the Actions tab and choose **Run workflow** on `main`.

If a deployment fails after the version has been raised, merging a fix will not start it again unless that merge also changes `pyproject.toml`. Once the fix is on `main`, open `Deploy docs` in the Actions tab and choose **Run workflow** on `main`.

If the error says that `uv.lock` needs to be updated, run `uv lock` and merge the change.

GitHub Pages must be set up once for this to work: in the repository's settings, under Pages, set the source to GitHub Actions.

## Checks

GitHub Actions runs the tests, the pre-commit checks and a build of the site on every pull request and on every push to `main`. The site is built from the parquet file in the repository, so this checks the code and pages, not fresh data. To run the tests and pre-commit checks locally, and to run the pre-commit checks before each commit:

```bash
uv run --group dev pytest
uv run --group dev pre-commit run --all-files
uv run --group dev pre-commit install
```
