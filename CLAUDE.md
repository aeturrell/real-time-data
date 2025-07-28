# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PyNowcast is a Python package for mixed frequency nowcasting that implements economic forecasting models including MIDAS (Mixed Data Sampling), Bridge models, and Dynamic Factor Models (DFM). The package provides tools for combining high-frequency (monthly) and low-frequency (quarterly) economic indicators to produce real-time predictions.

## Core Architecture

The package centers around three main modeling approaches:

- **MIDASRegressor** (`src/pynowcast/__init__.py:66-586`): Implements MIDAS regression using exponential Almon weights for mixed frequency data
- **MixedFrequencyBridge** (`src/pynowcast/__init__.py:1010-1158`): Bridge model for temporally aggregating high-frequency variables to match target frequency
- **Nowcast** (`src/pynowcast/__init__.py:1427-1708`): Main orchestration class that handles data preprocessing, transformation, and model fitting

Key data transformation utilities:
- **TimeSeriesDifference, TimeSeriesLog, TimeSeriesPct** (`src/pynowcast/__init__.py:1160-1276`): Scikit-learn compatible transformers for time series preprocessing
- **generate_economic_data** (`src/pynowcast/__init__.py:735-933`): Synthetic economic data generator for testing and examples

## Common Development Tasks

### Environment Setup
```bash
# Install dependencies
uv sync --group dev

# Install package in editable mode
uv pip install -e .
```

### Testing
```bash
# Run tests for all Python versions
nox -s tests

# Run tests for specific Python version
nox -s tests-3.11

# Run with coverage
nox -s coverage

# Run doctests
nox -s xdoctest
```

### Code Quality
```bash
# Run all pre-commit hooks
nox -s pre-commit

# Run specific hooks
pre-commit run ruff --all-files
pre-commit run ruff-format --all-files
```

### Documentation
```bash
# Build documentation site
make site

# Publish to GitHub Pages
make publish
```

### Type Checking
```bash
# Runtime type checking
nox -s typeguard
```

## Data Schema

The package expects economic data in a specific long format with schema validation via Pandera:
- `ref_date`: Reference date (when data refers to)
- `pub_date`: Publication date (when data was released)
- `variable`: Variable name (string)
- `value`: Numeric value (float)

## Key Configuration

- **Package structure**: Uses `src/` layout with single module in `src/pynowcast/`
- **Build system**: Uses setuptools with `pyproject.toml` configuration
- **Dependency management**: Uses `uv` for fast dependency resolution
- **Documentation**: Quarto-based documentation with auto-generated API reference using quartodoc
- **Testing**: pytest with coverage reporting and xdoctest for documentation examples
- **Code style**: Ruff for linting and formatting, pydoclint for docstring validation

## Important Notes

- The main module contains extensive functionality in a single file (`__init__.py`) with comprehensive docstrings and mathematical formulations
- Models are scikit-learn compatible with `fit`/`predict` interfaces
- The package includes plotting capabilities using matplotlib with custom plot styles
- All models support both in-sample prediction and visualization methods
- Documentation is automatically built from docstrings and includes mathematical notation using LaTeX

## Development Hints and Tips

Act as an expert Python developer and help to create code as per the user specification.

RULES:

- MUST provide clean, production-grade, high quality code.

- ASSUME the user is using python version 3.9+

- USE well-known python design patterns

- MUST provide code blocks with proper google style docstrings

- MUST provide code blocks with input and return value type hinting.

- MUST use type hints

- PREFER to use F-string for formatting strings

- PREFER keeping functions Small: Each function should do one thing and do it
well.

- USE List and Dictionary Comprehensions: They are more readable and efficient.

- USE generators for large datasets to save memory.

- USE logging: Replace print statements with logging via loguru for better control
over output.

- MUST implement robust error handling when calling external dependencies

- Ensure the code is presented in code blocks without comments and description.

- MUST put numbers into variables with meaningful names
