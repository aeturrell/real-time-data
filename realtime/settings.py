"""Project-wide settings: file locations and polite-download parameters."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRATCH_DIR = PROJECT_ROOT / "scratch"
CACHE_DIR = SCRATCH_DIR / "cache"
LOG_DIR = PROJECT_ROOT / "logs"
SERIES_CONFIG_PATH = PROJECT_ROOT / "series.toml"
DATA_ISSUES_PATH = PROJECT_ROOT / "data_issues.toml"
ARCHIVE_PATH = PROJECT_ROOT / "realtimedata.parquet"
OECD_GDP_GROWTH_PATH = PROJECT_ROOT / "docs" / "oecd_gdp_growth.csv"
DIAGNOSTICS_PATH = SCRATCH_DIR / "diagnostics.csv"
BENCHMARK_PATH = SCRATCH_DIR / "oecd_benchmark.csv"
LATE_FIRST_APPEARANCES_PATH = SCRATCH_DIR / "late_first_appearances.csv"

ONS_BASE_URL = "https://www.ons.gov.uk"
BANK_OF_ENGLAND_BASE_URL = "https://www.bankofengland.co.uk"
ONS_SEARCH_API_URL = "https://api.beta.ons.gov.uk/v1/search"
USER_AGENT = "real-time-data/0.1 (+https://github.com/aeturrell/real-time-data)"

LONDON_TIMEZONE = "Europe/London"
