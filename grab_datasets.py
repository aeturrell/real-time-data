import pandas as pd
import numpy as np
from pandas.tseries import offsets
import toml
from bs4 import BeautifulSoup
import requests
from pathlib import Path, PosixPath
from openpyxl import load_workbook
import xlrd
import re
import zipfile
import itertools
import os

# Read local `config.toml` file.
config = toml.load("config.toml")
print(config)


def get_sheetnames_xlsx(filepath: PosixPath):
    wb = load_workbook(filepath, read_only=True, keep_links=False)
    return wb.sheetnames


def get_sheetnames_xls(filepath: PosixPath):
    xls = xlrd.open_workbook(filepath, on_demand=True)
    return xls.sheet_names()


def remove_bad_sheets(series: pd.Series):
    return series.apply(lambda x: [el for el in x if "triangle" in el])


def find_files(url: str):
    soup = BeautifulSoup(requests.get(url).text, features="html5lib")

    hrefs = [a["href"] for a in soup.find_all("a")]
    hrefs = [a for a in hrefs if len(a.split(".")) > 1]
    hrefs = [
        a for a in hrefs if (a.split(".")[1] == "xlsx" or a.split(".")[1] == "xls" or a.split(".")[1] == "zip" or a.split(".")[1] == "xlsm")
    ]
    return hrefs


def download_zip_file(file_url: str, in_file_name: str):
    """Downloads a zip file from given url.

    :param file_url: url
    :type file_url: str
    :param in_file_name: zip file to download
    :type in_file_name: str
    :return: Name of the file actually downloaded
    :rtype: str
    """
    _ = download_and_save_file(file_url, in_file_name)
    names_to_keep = ["quarterly", "m on m"]
    file_location = Path("scratch") / in_file_name
    zip = zipfile.ZipFile(file_location)
    names = [name for name in zip.namelist()]
    files_to_extract = [[x for x in names if y in x.lower()] for y in names_to_keep]
    files_to_extract = list(itertools.chain(*files_to_extract))
    for file in files_to_extract:
        zip.extract(file, path=Path("scratch"))
    assert(len(files_to_extract) == 1)
    # Tidy up by removing the zip
    os.remove(file_location)
    return files_to_extract[0]


def download_and_save_file(file_url: str, file_name: str):
    # Download the file from the top of the list
    file_location = Path("scratch") / file_name
    if file_location.is_file():
        print("Skipping download; file already exists")
    else:
        r = requests.get("https://www.ons.gov.uk" + file_url, stream=True)
        with open(Path("scratch") / file_name, "wb") as f:
            f.write(r.content)
    print("Success: file downloaded")
    return file_name


def convert_yyyy_qn_to_datetime(series: pd.Series):
    return (
        pd.to_datetime(series.apply(lambda x: x[:4] + "-" + str(int(x[-1]) * 3)))
        + pd.offsets.QuarterEnd()
    )


def find_vintage_from_pub_datetime(df_in: pd.DataFrame):
    offsets = {
        "1st": pd.offsets.MonthEnd(),
        "M2": pd.offsets.MonthEnd(2),  # 2nd estimate (month 2)
        "QNA": pd.offsets.QuarterEnd(),
        "M3": pd.offsets.MonthEnd(3),
    }
    df_in["vintage"] = df_in.apply(
        lambda x: offsets[x["estimate"]] + x["pub_datetime"], axis=1
    )
    return df_in


def combined_df_urls(config):
    df_urls = pd.DataFrame()
    frequencies = ["Q", "M"]
    for freq in frequencies:
        df_urls = pd.concat(
            [df_urls, populate_dataframe_of_data_urls(config, freq)], axis=0
        )
    for key, value in config[freq][0].items():
        df_urls[key] = ""
    for freq in frequencies:
        for key, value in config[freq][0].items():
            if(key!="urls"):
                for inner_key, inner_val in value.items():
                    df_urls.loc[inner_key, key] = inner_val
    return df_urls


def populate_dataframe_of_data_urls(config, freq):
    dict_of_urls = config[freq][0]["urls"]
    dict_of_files = {k: find_files(v) for k, v in dict_of_urls.items()}
    # restrict to only first file found on each page
    for key, value in dict_of_files.items():
        dict_of_files[key] = value[0]
    # turn this into a dataframe
    df_urls = pd.DataFrame(dict_of_files, index=["url"]).T
    df_urls["file_name"] = df_urls["url"].apply(lambda x: x.split("/")[-1])
    df_urls[["url", "file_name"]].set_index("url").to_dict()
    df_urls["freq"] = freq
    df_urls["extension"] = df_urls["file_name"].str.split(".").str[1]
    return df_urls


def download_all_files(df_urls):
    df_urls["dl_filename"] = ""
    # Download non-zips
    query = df_urls["extension"] != "zip"
    df_urls.loc[query, "dl_filename"] = df_urls.loc[query, :].apply(lambda x: download_and_save_file(x["url"], x["file_name"]), axis=1)
    # Download zips
    df_urls.loc[~query, "dl_filename"] = df_urls.loc[~query, :].apply(lambda x: download_zip_file(x["url"], x["file_name"]), axis=1)
    df_urls["dl_fn_extension"] = df_urls["dl_filename"].str.split(".").str[1]
    return df_urls


def nominate_sheets_from_ss(df_urls):
    # Add sheet names
    df_urls["sheet_names"] = "None"
    df_urls.loc[df_urls["dl_fn_extension"] == "xlsx", "sheet_names"] = df_urls.loc[
        df_urls["dl_fn_extension"] == "xlsx", :
    ].apply(lambda x: get_sheetnames_xlsx(Path("scratch") / x["dl_filename"]), axis=1)
    if("xlsm" in df_urls["dl_fn_extension"].unique()):
        df_urls.loc[df_urls["dl_fn_extension"] == "xlsm", "sheet_names"] = df_urls.loc[
            df_urls["dl_fn_extension"] == "xlsm", :
        ].apply(lambda x: get_sheetnames_xlsx(Path("scratch") / x["dl_filename"]), axis=1)
    if "xls" in df_urls["dl_fn_extension"].unique():
        df_urls.loc[df_urls["dl_fn_extension"] == "xls", "sheet_names"] = df_urls.loc[
            df_urls["dl_fn_extension"] == "xls", :
        ].apply(lambda x: get_sheetnames_xls(Path("scratch") / x["dl_filename"]), axis=1)
    df_urls["sheet_names"] = remove_bad_sheets(df_urls["sheet_names"])
    # stick only to the first sheet
    df_urls["sheet_names"] = df_urls["sheet_names"].apply(lambda x: x[0])
    return df_urls


def process_triangle_file(df_urls_row):
    file_name, sheet_name = df_urls_row["dl_filename"], df_urls_row["sheet_names"]
    df = pd.read_excel(Path("scratch") / file_name, sheet_name=sheet_name)
    # Remove all the of the guff
    search_text = "Relating to Period"
    df = df.dropna(how="all", axis=1).dropna(how="all", axis=0)
    dates_row = df[df == search_text].dropna(how="all", axis=1).dropna(how="all", axis=0).index.values
    df = df.rename(columns=dict(zip(df.columns, df.loc[dates_row, :].values[0])))
     # remove any lingering first cols
    srch_txt_ix = list(df.columns).index(search_text)
    if(srch_txt_ix!=0):
        df = df[df.columns[srch_txt_ix:]].copy()
    df[df.columns[0]] = pd.to_datetime(df[df.columns[0]], errors="coerce")
    first_datetime_row = pd.to_datetime(df[df.columns[0]], errors="coerce").dropna().index.min()
    df = df.loc[first_datetime_row:, :]
    # fill in the "latest estimate" entry with a datetime
    df = df[~pd.isna(df[search_text])].copy()
    time_series_down = pd.to_datetime(df[search_text], errors="coerce")
    time_series_down.iloc[-1] = time_series_down.iloc[-2] + pd.DateOffset(months=3)
    df[search_text] = time_series_down
    df = pd.melt(df, id_vars=search_text, var_name="datetime")
    df = df.rename(columns={search_text: "vintage"})
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    if("Q" in str(df["datetime"].iloc[0])):
        df["datetime"] = convert_yyyy_qn_to_datetime(df["datetime"].str.strip())
    df = df.dropna(subset=["value"])
    other_vars_to_store = ["long_name", "code", "short_name", "measure"]
    for var in other_vars_to_store:
        df[var] = df_urls_row[var]
    return df


df_urls = combined_df_urls(config)
df_urls = download_all_files(df_urls)
df_urls = nominate_sheets_from_ss(df_urls)

# Testing
df_urls_row = df_urls.iloc[-1]
df = process_triangle_file(df_urls_row)
