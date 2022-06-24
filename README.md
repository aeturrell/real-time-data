# real-time-data

## Introduction

This repo aims to pull out as many real-time datasets (and their revision triangles) as possible from the ONS website and put them into a single, consistent (tidy) format in a parquet file that is generated in scratch/ when the code is run.

## Installing the Python environment

The Python environment is managed and tracked by [poetry](https://python-poetry.org/). To install it, use

```bash
poetry install
```

on the command line. To run the main script, "grab_datasets.py" from the command line, use `poetry run grab_datasets.py".

The environment requires a base installation of Python 3.9, which you can do via conda using `conda env create -f environment.yml`.

## Data Ingested

The different types of data to be ingested are tracked in a configuration (toml) file called config.toml. Each dataset has a frequency (eg M, Q) and then:

- a URL to where the file(s) live on the ONS website
- a long name
- a code name, usually the ONS' code for that variable
- a short name
- a measure (eg the units)

## Further Development

There are likely to be other useful real-time series that have not yet been added to the config. Note that there is a lot of inconsistency across series, so bespoke file readers and data transformers are often needed when adding new series.

Diagnostics of the quality of both whether series are pulled in and the quality of the pulled in series are needed. It would also be wise to set up a check against known partial databases such as the one hosted by the Bank of England.

Ultimately, this code could be run weekly via github actions and the data dumped into a SQL database that is then served up online via [datasette](https://datasette.io/).
