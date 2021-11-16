import pandas as pd
from pandas.tseries import offsets
import toml
from bs4 import BeautifulSoup
import requests
from pathlib import Path, PosixPath
from openpyxl import load_workbook
import xlrd
import re

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
    bad_names = [
        "Information",
        "NOTICES",
        "revisions",
        "comments",
        "after 12 Months",
        "Chart Titles",
    ]
    return series.apply(lambda x: [el for el in x if el not in bad_names])


def find_files(url: str):
    soup = BeautifulSoup(requests.get(url).text, features="html5lib")

    hrefs = [a["href"] for a in soup.find_all("a")]
    hrefs = [a for a in hrefs if len(a.split(".")) > 1]
    hrefs = [
        a for a in hrefs if (a.split(".")[1] == "xlsx" or a.split(".")[1] == "xls")
    ]
    return hrefs


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
    # Download the files
    df_urls.apply(lambda x: download_and_save_file(x["url"], x["file_name"]), axis=1)
    # add file extensions
    df_urls["extension"] = df_urls["file_name"].str.split(".").str[1]
    # Add sheet names
    df_urls["sheet_names"] = "None"
    df_urls.loc[df_urls["extension"] == "xlsx", "sheet_names"] = df_urls.loc[
        df_urls["extension"] == "xlsx", :
    ].apply(lambda x: get_sheetnames_xlsx(Path("scratch") / x["file_name"]), axis=1)
    if "xls" in df_urls["extension"].unique():
        df_urls.loc[df_urls["extension"] == "xls", "sheet_names"] = df_urls.loc[
            df_urls["extension"] == "xls", :
        ].apply(lambda x: get_sheetnames_xls(Path("scratch") / x["file_name"]), axis=1)
    df_urls["sheet_names"] = remove_bad_sheets(df_urls["sheet_names"])
    df_urls["freq"] = freq
    return df_urls


def combined_df_urls(config):
    df_urls = pd.DataFrame()
    for freq in ["Q", "M"]:
        df_urls = pd.concat(
            [df_urls, populate_dataframe_of_data_urls(config, freq)], axis=0
        )
    return df_urls


def process_quarterly_file(file_name, sheet_name):
    df = pd.read_excel(Path("scratch") / file_name, sheet_name=sheet_name)
    df = df.dropna(how="all", axis=1)
    if ":" in df.columns[1]:
        long_name, measure = df.columns[1].split(": ")
    else:
        long_name, measure = df.columns[1], ""
    df = df.dropna(how="all", axis=0)
    units = df.iloc[0, 1]
    df = df.iloc[2:, :]
    if "Annual" in df.iloc[0, 1]:
        df = df.iloc[1:, :]
    # account for case where there's an extra row for prices
    if any(df.iloc[0].fillna("").str.lower().str.contains("prices")):
        df.iloc[0] = df.iloc[0].ffill()  # forward fill empty prices cols
        df.columns = (
            df.loc[3].fillna("")
            + ";"
            + df.loc[4].fillna("")
            + ";"
            + df.loc[5].fillna("")
        )
        df = df.iloc[3:, :]
    else:
        df.columns = ";" + df.loc[3].fillna("") + ";" + df.loc[4].fillna("")
        df = df.iloc[2:, :]
    df = df.rename(columns={";;": "datetime"})
    df = pd.melt(df, id_vars="datetime")
    df["datetime"] = convert_yyyy_qn_to_datetime(df["datetime"].str.strip())
    df[["prices", "estimate", "pub_datetime"]] = df["variable"].str.split(
        ";", expand=True
    )
    df["pub_datetime"] = convert_yyyy_qn_to_datetime(df["pub_datetime"])
    # clean up est col
    df["estimate"] = df["estimate"].str.strip()
    df = find_vintage_from_pub_datetime(df)
    df[["long_name", "measure", "units", "code"]] = (
        long_name,
        measure,
        units,
        sheet_name,
    )
    # Set up a special case for the unpredictable case of Business Investment
    if "Total Business Investment" in long_name:
        _, long_name, measure = re.split(r"(.*?\s.*?\s.*?)\s", long_name)
        df["code"] = "NPEL"
        df["long_name"] = long_name
        df["measure"] = measure
    df["units"] = df["prices"] + " ; " + df["units"]
    return df.drop(["variable", "pub_datetime", "estimate", "prices"], axis=1)


def process_monthly_file(file_name, sheet_name):
    df = pd.read_excel(Path("scratch") / file_name, sheet_name=sheet_name)
    df = df.dropna(how="all", axis=1)
    df = df.dropna(how="all", axis=0)
    code = df.columns[0].split(":")[0]
    long_name = df.columns[0].split("for")[1].strip()
    measure = df.columns[0].split("for")[1].strip()
    df.columns = df.loc[4, :]
    df = df.iloc[1:, :]
    df = df.rename(
        columns={"Relating to Period (three months ending)": "Relating to Period"}
    )
    # fill in the "latest estimate" entry with a datetime
    time_series_down = pd.to_datetime(df["Relating to Period"], errors="coerce")
    time_series_down.iloc[-1] = time_series_down.iloc[-2] + pd.DateOffset(months=1)
    df["Relating to Period"] = time_series_down
    df = df[~pd.isna(df["Relating to Period"])]
    df = pd.melt(df, id_vars="Relating to Period", var_name="datetime")
    df = df.rename(columns={"Relating to Period": "vintage"})
    df["long_name"] = long_name
    df["measure"] = measure
    df["code"] = code
    return df


df_urls = combined_df_urls(config)

# Testing quarterly pipeline
file_name = df_urls["file_name"].iloc[2]
sheet_name = "2011 -"

df = process_quarterly_file(df_urls["file_name"].iloc[2], "2011 -")


# Testing monthly pipeline
file_name = df_urls["file_name"].iloc[-2]
sheet_name = "triangle"

df = process_monthly_file(file_name, sheet_name)
