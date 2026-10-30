"""Project-wide settings: file locations and polite-download parameters."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRATCH_DIR = PROJECT_ROOT / "scratch"
CACHE_DIR = SCRATCH_DIR / "cache"
LOG_DIR = PROJECT_ROOT / "logs"
SERIES_CONFIG_PATH = PROJECT_ROOT / "series.toml"
ARCHIVE_PATH = SCRATCH_DIR / "realtimedata.parquet"
DIAGNOSTICS_PATH = SCRATCH_DIR / "diagnostics.csv"
BENCHMARK_PATH = SCRATCH_DIR / "oecd_benchmark.csv"

ONS_BASE_URL = "https://www.ons.gov.uk"
ONS_SEARCH_API_URL = "https://api.beta.ons.gov.uk/v1/search"
USER_AGENT = "real-time-data/0.1 (+https://github.com/aeturrell/real-time-data)"

LONDON_TIMEZONE = "Europe/London"
