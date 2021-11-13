import pandas as pd
import toml
from bs4 import BeautifulSoup
import requests
from pathlib import Path

# Read local `config.toml` file.
config = toml.load('config.toml')
print(config)


def find_files(url):
    soup = BeautifulSoup(requests.get(url).text, features="html5lib")

    hrefs = [a["href"] for a in soup.find_all("a")]
    hrefs = [a for a in hrefs if len(a.split(".")) > 1]
    hrefs = [a for a in hrefs if a.split(".")[1] == "xlsx"]
    return hrefs

dict_of_urls = config["urls"]
dict_of_files = {k:find_files(v) for k, v in dict_of_urls.items()}

# Download a file
r = requests.get("https://www.ons.gov.uk" + dict_of_files["gdp_expenditure"][0], stream=True)
with open(Path('scratch/gdp_expenditure.xlsx'), 'wb') as f:
    f.write(r.content)

df = pd.read_excel(Path('scratch/gdp_expenditure.xlsx'), sheet_name="ABJR")
