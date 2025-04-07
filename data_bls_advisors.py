import os
import time
import zipfile
import pandas as pd
import json
import glob
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import chromedriver_autoinstaller

# Constants
SOC_CODE = "13-2052"
OUTPUT_DIR = "bls_advisors_data"
DATA_DICT_FILE = "data_dictionary.json"
BLS_URL = "https://www.bls.gov/oes/tables.htm"

# Data dictionary mapping
COLUMN_DEFINITIONS = {
    "area": "MSA area code",
    "area_title": "Area name",
    "area_type": "Area type",
    "prim_state": "Primary state for the area",
    "naics": "NAICS industry code",
    "naics_title": "NAICS industry title",
    "i_group": "Industry group level",
    "own_code": "Ownership type",
    "occ_code": "Occupation code (SOC)",
    "occ_title": "Occupation title",
    "o_group": "Occupation level",
    "tot_emp": "Total employment",
    "emp_prse": "Employment percent relative standard error",
    "jobs_1000": "Jobs per 1,000",
    "loc_quotient": "Location quotient",
    "pct_total": "Percent of industry employment",
    "pct_rpt": "Percent of establishments reporting",
    "h_mean": "Mean hourly wage",
    "a_mean": "Mean annual wage",
    "mean_prse": "Wage percent relative standard error",
    "h_pct10": "10th percentile hourly wage",
    "h_pct25": "25th percentile hourly wage",
    "h_median": "Median hourly wage",
    "h_pct75": "75th percentile hourly wage",
    "h_pct90": "90th percentile hourly wage",
    "a_pct10": "10th percentile annual wage",
    "a_pct25": "25th percentile annual wage",
    "a_median": "Median annual wage",
    "a_pct75": "75th percentile annual wage",
    "a_pct90": "90th percentile annual wage",
    "annual": "Annual wages only (TRUE/FALSE)",
    "hourly": "Hourly wages only (TRUE/FALSE)",
    "year": "Year of data"
}

def setup_browser(download_dir):
    chromedriver_autoinstaller.install()
    chrome_options = Options()
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": os.path.abspath(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def wait_for_download(filename, timeout=30):
    print("Waiting for download to complete...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        if os.path.exists(filename) and not any(glob.glob(filename + "*.crdownload")):
            return True
        time.sleep(1)
    print("Timeout while waiting for file to download.")
    return False

def detect_available_years(driver, max_back=5):
    print("Detecting available years from BLS OEWS page...")
    driver.get(BLS_URL)
    time.sleep(3)

    all_links = driver.find_elements(By.TAG_NAME, "a")
    zip_links = [l.get_attribute("href") for l in all_links if l.get_attribute("href") and "oesm" in l.get_attribute("href") and l.get_attribute("href").endswith("ma.zip")]

    years = []
    for link in zip_links:
        try:
            yy = link.split("oesm")[1][:2]
            full_year = 2000 + int(yy)
            years.append(full_year)
        except:
            continue

    years = sorted(set(years), reverse=True)[:max_back]
    print(f"Detected available years: {years}")
    return sorted(years)

def download_zip_for_year(driver, year, download_dir):
    print(f"Downloading ZIP for {year}...")
    driver.get(BLS_URL)
    time.sleep(3)

    yy = str(year)[-2:]
    expected_zip_filename = f"oesm{yy}ma.zip"

    all_links = driver.find_elements(By.TAG_NAME, "a")
    matching_links = [link for link in all_links if link.get_attribute("href") and expected_zip_filename in link.get_attribute("href")]

    if not matching_links:
        print(f"No ZIP link found for {year}")
        return None

    href = matching_links[0].get_attribute("href")
    print(f"Found ZIP link: {href}")
    driver.get(href)

    zip_path = os.path.join(download_dir, expected_zip_filename)

    if wait_for_download(zip_path):
        print(f"Downloaded: {zip_path}")
        return zip_path
    else:
        print(f"Failed to fully download {expected_zip_filename}")
        return None

def extract_and_filter(zip_path, year, output_path):
    with zipfile.ZipFile(zip_path, "r") as z:
        print(f"Contents of ZIP {os.path.basename(zip_path)}:", z.namelist())
        #file_list = [f for f in z.namelist() if f.lower().endswith(".xlsx") and "msa" in f.lower()]
        file_list = [
            f for f in z.namelist()
            if f.lower().endswith(".xlsx") and "msa" in f.lower() and not os.path.basename(f).startswith("~$")
        ]

        
        if not file_list:
            print(f"No valid MSA Excel file found in {zip_path}")
            return

        excel_name = file_list[0]
        with z.open(excel_name) as f:
            df = pd.read_excel(f, dtype=str, engine="openpyxl")

        df.columns = [col.strip().lower() for col in df.columns]

        if "occ_code" not in df.columns:
            print(f"'occ_code' column not found in {excel_name} — skipping.")
            return

        df = df[df["occ_code"] == SOC_CODE]
        df["year"] = year
        df.to_csv(output_path, index=False)
        print(f"Saved filtered data for {year} to {output_path}")

def generate_data_dictionary():
    dict_path = os.path.join(OUTPUT_DIR, DATA_DICT_FILE)
    with open(dict_path, "w") as f:
        json.dump(COLUMN_DEFINITIONS, f, indent=4)
    print(f"Generated data dictionary at {dict_path}")

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    download_dir = os.path.abspath(OUTPUT_DIR)

    driver = setup_browser(download_dir)
    years = detect_available_years(driver, max_back=5)

    for year in years:
        zip_file = download_zip_for_year(driver, year, download_dir)
        if zip_file:
            output_csv = os.path.join(OUTPUT_DIR, f"{year}.csv")
            extract_and_filter(zip_file, year, output_csv)

    driver.quit()
    generate_data_dictionary()
    print("All done.")

if __name__ == "__main__":
    main()
