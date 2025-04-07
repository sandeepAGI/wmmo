#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
data_irs_soi.py

This script downloads and processes IRS Statistics of Income (SOI) data at the ZIP code level.
It collects data for the past 5 years and saves both the raw data and a data dictionary
to the specified output directory.
"""

import os
import requests
import zipfile
import io
import pandas as pd
import logging
from datetime import datetime
import re
import time
import json
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("irs_soi_collection.log"),
        logging.StreamHandler()
    ]
)

# Constants
BASE_URL = "https://www.irs.gov/statistics/soi-tax-stats-individual-income-tax-statistics-zip-code-data-soi"
OUTPUT_DIR = "irs_soi_data"
YEARS_TO_COLLECT = 5  # Collect 5 years of data as requested

def ensure_directory_exists(directory):
    """Create directory if it doesn't exist"""
    if not os.path.exists(directory):
        os.makedirs(directory)
        logging.info(f"Created directory: {directory}")

def scrape_available_years():
    """Scrape the IRS website to identify available years of data"""
    try:
        response = requests.get(BASE_URL)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find links to specific year data pages
        year_links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            # Look for patterns like "2021-zip-code-data" in hrefs
            if re.search(r'(\d{4})-zip-code-data', href) or re.search(r'(\d{4}).*?zip-code', href.lower()):
                year = re.search(r'(\d{4})', href).group(1)
                year_links.append((year, urljoin(BASE_URL, href)))
        
        # Sort by year (descending) and take last 5 years
        year_links.sort(key=lambda x: x[0], reverse=True)
        recent_years = year_links[:YEARS_TO_COLLECT]
        
        logging.info(f"Found data for these years: {[year for year, _ in recent_years]}")
        return recent_years
    
    except Exception as e:
        logging.error(f"Error scraping available years: {e}")
        return []

def find_download_links(year_page_url):
    """Find CSV and documentation links on a specific year's page"""
    try:
        response = requests.get(year_page_url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Dictionary to store links
        links = {
            'csv_with_agi': None,
            'csv_without_agi': None,
            'zip_all': None,
            'documentation': None
        }
        
        # Find links for CSV files, ZIP files and documentation
        for link in soup.find_all('a', href=True):
            href = link['href']
            text = link.get_text().lower()
            
            # Check for documentation link
            if ('guide' in text or 'layout' in text or 'doc' in text) and (href.endswith('.doc') or href.endswith('.docx')):
                links['documentation'] = urljoin(year_page_url, href)
            
            # Check for ZIP file containing all data
            elif re.search(r'\.zip$', href, re.IGNORECASE):
                links['zip_all'] = urljoin(year_page_url, href)
                
            # Check for CSV file with AGI
            elif 'csv' in href.lower() and 'include' in text and 'agi' in text:
                links['csv_with_agi'] = urljoin(year_page_url, href)
                
            # Check for CSV file without AGI
            elif 'csv' in href.lower() and 'not include' in text and 'agi' in text:
                links['csv_without_agi'] = urljoin(year_page_url, href)
        
        return links
        
    except Exception as e:
        logging.error(f"Error finding download links: {e}")
        return {}

def download_file(url, local_filename):
    """Download a file from URL and save to local_filename"""
    if not url:
        return None
        
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(local_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        logging.info(f"Downloaded {url} to {local_filename}")
        return local_filename
        
    except Exception as e:
        logging.error(f"Error downloading {url}: {e}")
        return None

def extract_zip(zip_path, extract_to):
    """Extract ZIP file contents"""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        logging.info(f"Extracted {zip_path} to {extract_to}")
        return True
    except Exception as e:
        logging.error(f"Error extracting {zip_path}: {e}")
        return False

def parse_csv_data(csv_path, year):
    """Parse CSV data and return as DataFrame"""
    try:
        df = pd.read_csv(csv_path, low_memory=False)
        df['tax_year'] = year  # Add tax year as a column
        logging.info(f"Parsed CSV data from {csv_path}, shape: {df.shape}")
        return df
    except Exception as e:
        logging.error(f"Error parsing CSV data from {csv_path}: {e}")
        return None

def extract_data_dictionary(doc_path):
    """
    Extract field descriptions from documentation file.
    This is a placeholder - the actual implementation depends on the
    structure of the documentation file.
    """
    # This is a placeholder. In reality, you would need to:
    # 1. Check if the file is .doc or .docx
    # 2. Use appropriate library (e.g., python-docx for .docx, textract for .doc)
    # 3. Extract field definitions and create structured dictionary
    
    data_dictionary = {
        "STATE": "State FIPS code",
        "ZIPCODE": "5-digit ZIP code",
        "AGI_STUB": "Size of adjusted gross income (1=under $25,000, 2=$25,000-$50,000, etc.)",
        "N1": "Number of returns",
        "MARS1": "Number of single returns",
        "MARS2": "Number of joint returns",
        "MARS4": "Number of head of household returns",
        "PREP": "Number of returns with paid preparer's signature",
        "N2": "Number of exemptions",
        "NUMDEP": "Number of dependents",
        "A00100": "Adjusted gross income (AGI)",
        "N00200": "Number of returns with salaries and wages",
        "A00200": "Salaries and wages amount",
        "N00300": "Number of returns with taxable interest",
        "A00300": "Taxable interest amount",
        "N00600": "Number of returns with ordinary dividends",
        "A00600": "Ordinary dividends amount",
        "N00900": "Number of returns with business/professional net income/loss",
        "A00900": "Business/professional net income/loss amount",
        "N01000": "Number of returns with net capital gain/loss",
        "A01000": "Net capital gain/loss amount",
        "N02300": "Number of returns with unemployment compensation",
        "A02300": "Unemployment compensation amount",
        "N18425": "Number of returns with state/local income taxes",
        "A18425": "State/local income taxes amount",
        "N18450": "Number of returns with real estate taxes",
        "A18450": "Real estate taxes amount",
        "N19300": "Number of returns with charitable contributions",
        "A19300": "Charitable contributions amount",
        "N04470": "Number of returns with alternative minimum tax",
        "A04470": "Alternative minimum tax amount",
        "N11901": "Number of returns with total tax payments",
        "A11901": "Total tax payments amount",
    }
    
    # Create a DataFrame from the dictionary
    dict_df = pd.DataFrame(list(data_dictionary.items()), columns=['Field', 'Description'])
    return dict_df

def create_complete_dictionary():
    """
    Create a comprehensive data dictionary that includes field definitions,
    data types, and additional metadata
    """
    # Basic field information - would ideally be parsed from documentation
    field_info = {
        "STATE": {"description": "State FIPS code", "type": "string", "notes": "Two-digit FIPS code"},
        "ZIPCODE": {"description": "5-digit ZIP code", "type": "string", "notes": ""},
        "AGI_STUB": {
            "description": "Size of adjusted gross income",
            "type": "integer",
            "notes": "Categories: 1=under $25,000, 2=$25,000-$50,000, 3=$50,000-$75,000, 4=$75,000-$100,000, 5=$100,000-$200,000, 6=$200,000+"
        },
        "N1": {"description": "Number of returns", "type": "integer", "notes": "Approximates number of households"},
        "MARS1": {"description": "Number of single returns", "type": "integer", "notes": "Filing status = single"},
        "MARS2": {"description": "Number of joint returns", "type": "integer", "notes": "Filing status = married filing jointly"},
        "MARS4": {"description": "Number of head of household returns", "type": "integer", "notes": "Filing status = head of household"},
        "PREP": {"description": "Number of returns with paid preparer's signature", "type": "integer", "notes": ""},
        "N2": {"description": "Number of exemptions", "type": "integer", "notes": "Approximates population count"},
        "NUMDEP": {"description": "Number of dependents", "type": "integer", "notes": ""},
        "A00100": {"description": "Adjusted gross income (AGI)", "type": "integer", "notes": "In thousands of dollars"},
        "N00200": {"description": "Number of returns with salaries and wages", "type": "integer", "notes": ""},
        "A00200": {"description": "Salaries and wages amount", "type": "integer", "notes": "In thousands of dollars"},
        "N00300": {"description": "Number of returns with taxable interest", "type": "integer", "notes": ""},
        "A00300": {"description": "Taxable interest amount", "type": "integer", "notes": "In thousands of dollars"},
        "N00600": {"description": "Number of returns with ordinary dividends", "type": "integer", "notes": ""},
        "A00600": {"description": "Ordinary dividends amount", "type": "integer", "notes": "In thousands of dollars"},
        "N00900": {"description": "Number of returns with business/professional net income/loss", "type": "integer", "notes": ""},
        "A00900": {"description": "Business/professional net income/loss amount", "type": "integer", "notes": "In thousands of dollars"},
        "N01000": {"description": "Number of returns with net capital gain/loss", "type": "integer", "notes": ""},
        "A01000": {"description": "Net capital gain/loss amount", "type": "integer", "notes": "In thousands of dollars"},
        "N01400": {"description": "Number of returns with IRA distributions", "type": "integer", "notes": ""},
        "A01400": {"description": "IRA distributions amount", "type": "integer", "notes": "In thousands of dollars"},
        "N01700": {"description": "Number of returns with pensions and annuities", "type": "integer", "notes": ""},
        "A01700": {"description": "Pensions and annuities amount", "type": "integer", "notes": "In thousands of dollars"},
        "N02300": {"description": "Number of returns with unemployment compensation", "type": "integer", "notes": ""},
        "A02300": {"description": "Unemployment compensation amount", "type": "integer", "notes": "In thousands of dollars"},
        "N02500": {"description": "Number of returns with social security benefits", "type": "integer", "notes": ""},
        "A02500": {"description": "Social security benefits amount", "type": "integer", "notes": "In thousands of dollars"},
        "N18425": {"description": "Number of returns with state/local income taxes", "type": "integer", "notes": ""},
        "A18425": {"description": "State/local income taxes amount", "type": "integer", "notes": "In thousands of dollars"},
        "N18450": {"description": "Number of returns with real estate taxes", "type": "integer", "notes": ""},
        "A18450": {"description": "Real estate taxes amount", "type": "integer", "notes": "In thousands of dollars"},
        "N19300": {"description": "Number of returns with charitable contributions", "type": "integer", "notes": ""},
        "A19300": {"description": "Charitable contributions amount", "type": "integer", "notes": "In thousands of dollars"},
        "N04470": {"description": "Number of returns with alternative minimum tax", "type": "integer", "notes": ""},
        "A04470": {"description": "Alternative minimum tax amount", "type": "integer", "notes": "In thousands of dollars"},
        "N11901": {"description": "Number of returns with total tax payments", "type": "integer", "notes": ""},
        "A11901": {"description": "Total tax payments amount", "type": "integer", "notes": "In thousands of dollars"},
    }
    
    # Additional metadata
    metadata = {
        "source": "IRS Statistics of Income (SOI)",
        "dataset": "Individual Income Tax Statistics - ZIP Code Data",
        "url": "https://www.irs.gov/statistics/soi-tax-stats-individual-income-tax-statistics-zip-code-data-soi",
        "notes": [
            "Data suppressed when the frequency is less than 20 returns to prevent disclosure of information about specific taxpayers.",
            "Money amounts are in thousands of dollars.",
            "'N' fields represent counts of returns, 'A' fields represent dollar amounts.",
            "AGI_STUB represents income categories.",
            "Returns by ZIP code do not add to totals due to exclusion of ZIP codes with less than 100 returns."
        ],
        "field_naming_convention": {
            "N prefix": "Count of returns",
            "A prefix": "Amount fields (in thousands of dollars)",
            "MARS prefix": "Filing status counts"
        }
    }
    
    # Create dictionary object with both field info and metadata
    data_dictionary = {
        "fields": field_info,
        "metadata": metadata,
        "last_updated": datetime.now().strftime("%Y-%m-%d")
    }
    
    return data_dictionary

def main():
    """Main function to process IRS SOI ZIP code data"""
    start_time = time.time()
    logging.info("Starting IRS SOI data collection")
    
    # Create output directory
    ensure_directory_exists(OUTPUT_DIR)
    
    # Get available years
    available_years = scrape_available_years()
    if not available_years:
        logging.error("Could not find any available years of data. Exiting.")
        return
    
    # Process each year's data
    all_data = []
    for year, year_url in available_years:
        year_dir = os.path.join(OUTPUT_DIR, f"tax_year_{year}")
        ensure_directory_exists(year_dir)
        
        logging.info(f"Processing data for tax year {year}")
        
        # Find download links
        download_links = find_download_links(year_url)
        if not download_links:
            logging.error(f"Could not find download links for tax year {year}")
            continue
        
        # Download and process files
        csv_path = None
        
        # Prefer CSV with AGI if available
        if download_links['csv_with_agi']:
            csv_file = os.path.join(year_dir, f"irs_soi_zipcode_{year}_with_agi.csv")
            csv_path = download_file(download_links['csv_with_agi'], csv_file)
        
        # If CSV with AGI not available or download failed, try ZIP file
        if not csv_path and download_links['zip_all']:
            zip_file = os.path.join(year_dir, f"irs_soi_zipcode_{year}_all.zip")
            zip_path = download_file(download_links['zip_all'], zip_file)
            
            if zip_path:
                # Extract ZIP file
                extract_zip(zip_path, year_dir)
                
                # Look for CSV files in extracted contents
                for file in os.listdir(year_dir):
                    if file.endswith('.csv') and 'agi' in file.lower():
                        csv_path = os.path.join(year_dir, file)
                        break
        
        # Download documentation
        if download_links['documentation']:
            doc_file = os.path.join(year_dir, f"irs_soi_zipcode_{year}_documentation.docx")
            download_file(download_links['documentation'], doc_file)
        
        # Process CSV data if available
        if csv_path and os.path.exists(csv_path):
            df = parse_csv_data(csv_path, year)
            if df is not None:
                # Save processed data
                processed_file = os.path.join(year_dir, f"irs_soi_zipcode_{year}_processed.csv")
                df.to_csv(processed_file, index=False)
                logging.info(f"Saved processed data to {processed_file}")
                
                # Append to all data
                all_data.append(df)
        else:
            logging.error(f"Could not process CSV data for tax year {year}")
    
    # Combine all years' data if available
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_file = os.path.join(OUTPUT_DIR, "irs_soi_zipcode_all_years.csv")
        combined_df.to_csv(combined_file, index=False)
        logging.info(f"Saved combined data from all years to {combined_file}")
    
    # Create data dictionary
    data_dict = create_complete_dictionary()
    dict_file = os.path.join(OUTPUT_DIR, "irs_soi_data_dictionary.json")
    with open(dict_file, 'w') as f:
        json.dump(data_dict, f, indent=4)
    logging.info(f"Saved data dictionary to {dict_file}")
    
    # Create data dictionary as CSV for easier viewing
    dict_df = pd.DataFrame([(k, v['description'], v['type'], v['notes']) 
                           for k, v in data_dict['fields'].items()],
                           columns=['Field', 'Description', 'Type', 'Notes'])
    dict_csv = os.path.join(OUTPUT_DIR, "irs_soi_data_dictionary.csv")
    dict_df.to_csv(dict_csv, index=False)
    
    # Create README file
    readme_content = f"""# IRS Statistics of Income (SOI) ZIP Code Data

This directory contains IRS SOI ZIP Code data collected on {datetime.now().strftime('%Y-%m-%d')}.

## Data Overview
- Data source: IRS Statistics of Income (SOI) Individual Income Tax Statistics
- Geographic level: ZIP Code
- Years available: {', '.join([year for year, _ in available_years])}

## Directory Structure
- `tax_year_XXXX/`: Directories containing data for specific tax years
- `irs_soi_zipcode_all_years.csv`: Combined data from all collected years
- `irs_soi_data_dictionary.json`: Data dictionary in JSON format
- `irs_soi_data_dictionary.csv`: Data dictionary in CSV format

## Field Naming Convention
- Fields starting with 'N' represent counts of returns
- Fields starting with 'A' represent dollar amounts (in thousands of dollars)
- Fields starting with 'MARS' represent filing status counts

## Notes
- Data may be suppressed for ZIP codes with fewer than 20 returns to prevent disclosure of information about specific taxpayers
- Money amounts are in thousands of dollars
- Returns by ZIP code do not add to totals due to exclusion of ZIP codes with less than 100 returns

For more information, visit the IRS website: https://www.irs.gov/statistics/soi-tax-stats-individual-income-tax-statistics-zip-code-data-soi
"""
    
    readme_file = os.path.join(OUTPUT_DIR, "README.md")
    with open(readme_file, 'w') as f:
        f.write(readme_content)
    logging.info(f"Saved README to {readme_file}")
    
    # Log completion
    elapsed_time = time.time() - start_time
    logging.info(f"IRS SOI data collection completed in {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    main()