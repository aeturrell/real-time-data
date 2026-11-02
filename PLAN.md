# Plan: expanding the real-time archive

This plan sets out how to grow the archive from 18 series to about 50 headline UK macroeconomic series, with real-time history going back to 1997. It covers phases 1 to 4 only.

Terms used throughout:

- **Vintage date**: the date a value was published (a calendar day).
- **Reference period**: the period a value describes, for example 2024 Q1 or March 2024. It is stored as a proper period, not a timestamp.
- **Change-only storage**: a row is kept only when a value for a series and reference period is first published, or when a later release changes it. This is how ALFRED stores data. Any past vintage can be rebuilt from these rows.
- **CDID**: the four-character ONS code that identifies a single time series, for example `ABMI` for GDP.

## What we found when looking at the ONS website (October 2026)

These checks shaped the plan, so they are recorded here.

1. **Every past release of a single series is kept online.** For example, `/economy/grossdomesticproductgdp/timeseries/abmi/qna/previous/v1/data` to `/v44/data` returns JSON, and each version includes a `releaseDate`. Version 1 dates from September 2015.
2. **Every past release of each whole dataset is kept online.** MM23 (prices) has 138 past versions, LMS (labour market) has 145 and QNA (quarterly national accounts) has 58. Each past CSV includes a `Release Date` row for every series, for example `13-10-2015`.
3. **ONS publishes 58 revision-triangle datasets.** Beyond the ones we already use, these include employment rate (EMP05), balance of payments, labour productivity, producer prices (JVZ7, K646), services producer prices, the trade balance, and GDP Annexes E to H (output, expenditure, income and sector accounts revisions). Triangles are the main route to history before 2015.
4. **The National Archives web archive can be searched by machine.** Its CDX index works (`https://webarchive.nationalarchives.gov.uk/ukgwa/cdx?url=...`). It holds old ONS dataset downloads, for example MM23 CSVs captured in about 30 different months between 2011 and 2016, and old `statistics.gov.uk` time-series pages from 2002 to 2011. Coverage is patchy, and most of the older pages are HTML forms rather than data files.
5. **The ONS API (`api.beta.ons.gov.uk`) has versioned datasets with release dates** that are relevant here: `gdp-to-four-decimal-places`, `output-in-the-construction-industry`, `retail-sales-index`, `labour-market` and `cpih01`.
6. **How past versions of a page are dated (found during Phase 1).** In a page's version list, a version's `updateDate` is the moment it was *replaced*. So the replacement time of version N-1 is when version N went live. A version's own `releaseDate` is less reliable. Versions created before the current website launched (25 February 2016) are migration leftovers with unreliable dates; they get an `upper_bound` vintage date. A series also appears on several dataset pages (GDP is on PGDP, PN2, QNA, UKEA and BB), and all of them are collected.
7. **Problems in the current archive:**
   - `abjr` and `npqt` point at the same URL.
   - `abjr`'s `code` sits under `[M]` in `config.toml` rather than `[Q]`.
   - `con`, `rsi`, `exp` and `imp` stop in 2023, because the old code took the first file link on each page, which was often a stale file. (Checked during Phase 1: `iop` and `iom` only looked identical because their files have the same shape. Their values differ.)
   - The four "non-revised" series use the observation date as the vintage, so they have no real revision history.
   - Triangle vintages are approximate: the "latest estimate" row is dated as the previous row plus three months.

## Series to add

These are the series a central bank would use to nowcast UK GDP, chosen to sit alongside the existing 18. All CDIDs and their home datasets were checked against the ONS search API.

| # | CDID | Dataset | Freq | Series |
|---|------|---------|------|--------|
| 1 | ABMI | QNA | Q | GDP, chained volume measure (CVM), seasonally adjusted (SA), £m |
| 2 | YBHA | QNA | Q | GDP at market prices, current prices, SA, £m |
| 3 | YBGB | QNA | Q | GDP implied deflator, SA |
| 4 | ECY2 | MGDP | M | Monthly GVA index, CVM SA |
| 5 | ECYX | MGDP | M | Monthly GVA, month-on-month growth, CVM SA |
| 6 | S2KU | IOS1 | M | Index of Services, total |
| 7 | K222 | DIOP | M | Index of Production, B–E |
| 8 | K22A | DIOP | M | Index of Production, manufacturing |
| 9 | J5EK | DRSI | M | Retail sales volume, all retailing including fuel, SA |
| 10 | NMRY | QNA | Q | Government final consumption, CVM SA |
| 11 | NPEL | CXNV | Q | Business investment, CVM SA |
| 12 | CAFU | QNA | Q | Change in inventories, CVM SA |
| 13 | ABJQ | UKEA | Q | Household consumption, current prices, SA |
| 14 | IKBK | MRET | M | Exports of goods and services, CVM SA |
| 15 | IKBL | MRET | M | Imports of goods and services, CVM SA |
| 16 | IKBJ | MRET | M | Trade balance, current prices, SA |
| 17 | HBOP | PNBP | Q | Current account balance, SA |
| 18 | NRJR | QNA | Q | Real household disposable income, CVM SA |
| 19 | NRJS | QNA | Q | Household saving ratio |
| 20 | LF24 | LMS | M | Employment rate, 16–64 |
| 21 | MGRZ | LMS | M | Employment level, 16+ |
| 22 | MGSC | LMS | M | Unemployment level, 16+ |
| 23 | LF2S | LMS | M | Economic inactivity rate, 16–64 |
| 24 | YBUS | LMS | M | Total actual weekly hours worked |
| 25 | AP2Y | LMS | M | Vacancies, total |
| 26 | KAB9 | LMS | M | Average weekly earnings (AWE), total pay level |
| 27 | KAI9 | LMS | M | AWE regular pay, 3-month average growth on a year earlier |
| 28 | KAJ4 | LMS | M | AWE private sector regular pay growth |
| 29 | D7BT | MM23 | M | CPI index, all items |
| 30 | L55O | MM23 | M | CPIH annual rate |
| 31 | DKO8 | MM23 | M | Core CPI annual rate (excluding energy, food, alcohol and tobacco) |
| 32 | D7NN | MM23 | M | CPI services annual rate |
| 33 | CHAW | MM23 | M | RPI all-items index |
| 34 | J5II | PUSF | M | Public sector net borrowing, excluding public sector banks |
| 35 | LZVD | PRDY | Q | Output per hour, change on a year earlier |

All 35 checked out, so the list runs a little over the 30 asked for. Items 13, 28, 33 and 35 can be dropped if a tighter list is wanted.

The existing triangle series will also be mapped to their proper CDIDs wherever one exists (for example `ihyq` becomes `IHYQ`), so that triangle and dataset sources join cleanly.

## Data model: one tidy table

Everything stays in one table and one parquet file. Descriptive fields are categorical columns. Repeating them on every row costs almost nothing, because pandas categoricals and parquet's dictionary encoding store each distinct value only once.

| Column | Type | Meaning |
|---|---|---|
| `series_id` | category | CDID, or our own code where ONS has none |
| `dataset_id` | category | ONS dataset, for example `QNA` |
| `title` | category | ONS series title |
| `short_name` | category | Our short label |
| `measure` | category | Units and adjustment, for example "£m; CVM; SA" |
| `frequency` | category | `A`, `Q` or `M` |
| `reference_period` | period (see below) | The period the value describes |
| `reference_period_basis` | category | `period` (a normal period) or `3m_ending` (for example, labour market rolling three-month figures) |
| `vintage_date` | `datetime64[ns]` at day resolution | When ONS published the value |
| `vintage_date_precision` | category | `day` for a known release date, `month` when only the publication month is known |
| `value` | float64 | Missing means the value was withdrawn in this vintage |
| `source` | category | `series_json`, `dataset_csv`, `triangle`, `realtime_db`, `web_archive` or `ons_api` |
| `source_url` | category | The exact file or endpoint used |
| `retrieved_at` | timestamp | When we downloaded it |
| `content_sha256` | category | Hash of the raw file the value came from |

### Storing reference periods for mixed frequencies

A pandas `PeriodDtype` column must have a single frequency, and parquet cannot store a column that mixes `Period` objects. The approach:

- **On disk**, `reference_period` is a categorical string in a form that `pd.Period` reads without ambiguity: `"2024"` (annual), `"2024Q1"` (quarterly), `"2024-03"` (monthly).
- **In Python**, `read_archive()` turns that column into `pd.Period` objects (object dtype), so one table can hold every frequency.
- **Helpers:**
  - `to_period_dtype(df)` gives a true `PeriodDtype` column when the frame has a single frequency.
  - `convert_frequency(df, "Q")` uses `Period.asfreq` to map periods between frequencies, so monthly series can be lined up against quarterly ones.
  - `period_start` and `period_end` are available as computed properties for any tool that needs plain dates.

### Change-only rules

- The key is (`series_id`, `reference_period`, `vintage_date`).
- A new row is written only when the value differs from the most recent earlier vintage for that series and period. Values are compared at the precision ONS published them, so that floating-point noise does not count as a revision.
- If a period disappears from a later vintage, a row with a missing value marks the withdrawal.
- When two sources report the same series, period and vintage, the one with exact release dates wins. The order of preference is `series_json` / `dataset_csv`, then `ons_api`, then `triangle`, then `realtime_db`, then `web_archive`. Disagreements between sources are logged.
- `as_of(df, vintage_date)` rebuilds the data as it stood on any date, and `triangle(df, series_id)` rebuilds a revisions triangle.

## Architecture

```
realtime/
  __init__.py
  cli.py            entry point: build, update, rebuild-offline, report
  config.py         loads and validates series.toml
  http.py           one requests.Session: retries, backoff on 429, rate limit, honest user agent
  cache.py          raw-file store keyed by URL and version, with a manifest of hash and retrieved_at
  schema.py         column definitions, period parsing, validation
  store.py          change-only merge, read_archive, as_of, triangle, convert_frequency
  release_dates.py  vintage-date lookup built from every source that carries release dates
  sources/
    base.py         Source protocol plus a registry
    series_json.py  per-series past versions (2015 onwards)
    dataset_csv.py  whole-dataset past versions (2015 onwards)
    triangle.py     revision-triangle spreadsheets (current and past editions)
    realtime_db.py  ONS GDP real-time databases
    web_archive.py  National Archives snapshots
    ons_api.py      api.beta.ons.gov.uk versioned datasets
tests/
  fixtures/         small trimmed copies of real files
```

- **Configuration.** `series.toml` replaces `config.toml`. It has one entry per series (CDID, dataset, frequency, short name, measure, basis), plus a list of triangle and real-time-database sources with hints for the parser.
- **Repeatable runs.** Past versions on the ONS site never change, so once they are cached they are kept for good. Only "current" pages are fetched again. `rebuild-offline` rebuilds the parquet file from the cache alone, and the result should be byte-for-byte the same. The manifest is a small parquet or JSON file in `scratch/cache/`.
- **Incremental updates.** The `update` command lists versions, downloads only the ones not seen before, and merges them in using the change-only rules.
- **Error handling.** Each source runs on its own: a failure in one is logged and the rest carry on. Each run ends with a summary of what succeeded and what failed.
- **Dependencies to add:** `pytest`. Drop `jupyterlab` and `fastparquet` from runtime dependencies, keeping `pyarrow`.

## Phase 1: foundations and moving the existing series across

Goal: today's 18 series rebuilt on the new structure, with correct vintage dates and reference periods.

1. Create the package layout, `http.py`, `cache.py` and the manifest.
2. Write `schema.py` with period parsing for every label style ONS uses (`"2024 Q1"`, `"2024 MAR"`, `"Mar-24"`, `"2024"`, and three-month-ending labels), plus validation of the schema.
3. Write `store.py`: the change-only merge, `read_archive`, `as_of`, `triangle` and `convert_frequency`.
4. Write `release_dates.py`. Start with release dates taken from series JSON (`description.releaseDate` for each version). Where no exact date is known, fall back to the publication month with `vintage_date_precision = month`.
5. Port the triangle parser into `sources/triangle.py`, fixing:
   - the `iop`/`iom` file mix-up
   - the duplicate `abjr`/`npqt` URL
   - the misplaced `abjr` code
   - the guessed "latest estimate" date, which should instead be looked up in the release-date table
6. Fix `con`, `rsi`, `exp` and `imp`: find where their pages have moved, or why the files changed format.
7. Remove the fake vintages for the "non-revised" series. Those series come back properly in Phase 2.
8. Write `cli.py` and a short README section.
9. Add tests: period parsing, change-only merge (new, revised, unchanged and withdrawn values), `as_of`, and one fixture file per triangle layout.

Done when: `build` produces the new parquet file for the existing triangle series; `rebuild-offline` reproduces it exactly; and the tests pass.

## Phase 2: past versions from 2015 onwards for all series

Goal: full real-time history from about September 2015 for the 35 new series, plus the existing four non-revised ones (D7G7, GHIP, GB7S, KAC2).

1. `sources/series_json.py`: for each CDID, list `/timeseries/{cdid}/{dataset}/previous`, then download each `/previous/vN/data` and `/data`. Read `years`, `quarters` and `months` into rows, taking the vintage from `description.releaseDate`. This is about 39 series × roughly 50–150 versions, so a few thousand small requests: under an hour on first run, cached afterwards.
2. Check whether a series' version list skips releases. ABMI has 44 series versions but QNA has 58 dataset versions. Write `sources/dataset_csv.py` for one dataset (QNA) and compare the two sources over 2015 onwards. If the series route misses releases where values changed, use dataset CSVs for the affected datasets. Dataset CSVs are larger, but one download covers every series in the dataset.
3. Add every release date seen to the release-date table, so triangle vintages from Phase 1 can be given exact days wherever their month matches.
4. Write a diagnostics report: for each series, the number of vintages, first and last vintage dates, gaps longer than the series' normal release interval, and source disagreements.

Done when: every series in the table above has vintages from 2015 to now with day precision, and the diagnostics report shows no unexplained gaps.

## Phase 3: history back to 1997

Goal: real-time history from 1997 onwards for as many of the series as possible. The diagnostics report says plainly where this is not achievable.

1. **More triangles.** Add EMP05 (employment rate), balance of payments triangles, labour productivity, PPI (JVZ7, K646), the trade balance, and GDP Annexes E, F, G and H. These cover most of the national accounts and labour market series.
2. **Past editions of triangles.** List each triangle page's past versions. Older editions sometimes cover periods or series that later editions dropped.
3. **ONS real-time databases.** Parse the GDP expenditure-components and income-components real-time databases (the files already sitting in `scratch/`, such as `gdpexpenditurecomponentsrealtimedatabase3.xlsx` and `gdpincomecomponentsrealtimedatabase1.xls`). These give QNA vintages back to the 1990s.
4. **Series that are never revised.** RPI is never revised, and CPI is revised only in rare, documented cases. For these, the current value is the first-published value. Each value gets a single vintage, its release date, from the release-date table, provided it was published after the series launched. Values before launch (for example CPI before 1997, or CPIH before 2013) were calculated later, so their vintage is the date they were first published as back data, not the month they describe.
5. **National Archives snapshots.**
   1. Use the CDX index to list captures of ONS dataset downloads (`/ons/datasets-and-tables/downloads/csv.csv?dataset=...`) and old `statistics.gov.uk` time-series downloads.
   2. Keep only captures with status 200 whose content is really CSV or zip, with duplicates removed by content hash.
   3. Read the release date from the file where it is present; otherwise use the capture date with `month` precision, which is an upper bound.
   4. This mainly fills 2011 to 2015 for the dataset-based series.
6. **Release dates before 2015.** Add dates from archived ONS release calendars and from release dates found inside archived files.

Done when: the diagnostics report shows, for each series, real-time coverage starting from 1997 or the earliest year achievable, along with the share of vintages that have day precision.

## Phase 4: the ONS API

Goal: add API-versioned data where it adds vintages or precision.

1. `sources/ons_api.py`: list editions and versions with `release_date`, then download the CSV for each version. Start with `gdp-to-four-decimal-places` (monthly GDP), `output-in-the-construction-industry`, `retail-sales-index`, `labour-market` and `cpih01`.
2. Map the API's dimension codes to our `series_id` values. Where an API series has no CDID, give it a stable code of our own and record it in `series.toml`.
3. Cross-check against Phase 2 values for the same vintage and log any differences.

Done when: API sources merge cleanly with no unexplained disagreements, and construction output has a proper level series in addition to the existing triangle.

## Risks and open questions

- **ONS rate limits.** These are not documented. The HTTP layer starts at about one request per second and backs off on 429 responses. Building the cache once is slow; runs after that are quick.
- **Pre-2011 coverage for series without triangles** (for example CPI services, vacancies and hours) may stay thin. Vacancies only start in 2001.
- **Rebasing and reclassification.** A CDID's units can change, for example when an index is rebased. Each vintage keeps the units published at the time, and the diagnostics report flags breaks in units.
- **Three-month-ending labour market series.** These are stored by the month the window ends, with `reference_period_basis = 3m_ending`, rather than being forced into quarters.
