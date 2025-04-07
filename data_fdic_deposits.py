#!/usr/bin/env python3
"""
FDIC Summary of Deposits (SOD) Data Collection Script

This script downloads and processes the FDIC Summary of Deposits data,
which contains deposit information for all branches and offices of 
FDIC-insured institutions. The data is collected at the lowest level of 
granularity available (branch level).

Usage:
    python data_fdic_deposit.py [--api_key YOUR_API_KEY] [--years YYYY,YYYY]
"""

import os
import sys
import argparse
import json
import csv
import requests
import pandas as pd
import zipfile
import io
import logging
from datetime import datetime
from pathlib import Path
from tqdm import tqdm  # For progress bars

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('fdic_data_collector')

# Constants
BASE_API_URL = "https://banks.data.fdic.gov/api"
SOD_ENDPOINT = "/sod"
OUTPUT_DIR = "fdic_deposit_data"
DATA_DICTIONARY_FILE = "fdic_sod_data_dictionary.csv"
BULK_DOWNLOAD_BASE_URL = "https://www7.fdic.gov/sod/sodMarket.asp?barItem=3"
START_YEAR = datetime.now().year - 5  # Last 5 years
END_YEAR = datetime.now().year - 1    # Previous complete year
DOWNLOAD_CHUNK_SIZE = 8192  # Chunk size for downloading large files

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Download FDIC Summary of Deposits data")
    parser.add_argument("--api_key", help="API key for FDIC API (may be required in future)")
    parser.add_argument("--years", help="Comma-separated list of years to download (e.g., 2019,2020,2021)")
    return parser.parse_args()

def setup_environment(api_key=None):
    """Set up environment and create directories."""
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Set API key if provided
    if api_key:
        os.environ["FDIC_API_KEY"] = api_key
    
    # Check if API key is in environment
    api_key = os.environ.get("FDIC_API_KEY", "")
    
    return api_key

def get_all_sod_data_for_year(year, api_key=""):
    """
    Get all SOD data for a specific year using the BankFind Suite API.
    Handles pagination to ensure all records are retrieved.
    
    Args:
        year (int): The year to download data for
        api_key (str): Optional API key
        
    Returns:
        pandas.DataFrame: DataFrame containing all the records, or None if failed
    """
    logger.info(f"Fetching complete SOD data for year {year} via API...")
    
    # Construct API request URL
    url = f"{BASE_API_URL}{SOD_ENDPOINT}"
    
    # Initial parameters - don't limit results, and use standard filters
    params = {
        "filters": f"YEAR:{year}",
        "format": "json",
        "limit": 10000,  # Request a large batch of records
        "offset": 0      # Start at the beginning
    }
    
    # Set up headers
    headers = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    
    all_data = []
    more_data = True
    
    try:
        with tqdm(desc=f"Fetching {year} data", unit=" records") as pbar:
            while more_data:
                logger.debug(f"Requesting data from {url} with offset {params['offset']}")
                
                response = requests.get(url, headers=headers, params=params)
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        
                        if 'data' in data and isinstance(data['data'], list):
                            batch_size = len(data['data'])
                            
                            if batch_size == 0:
                                # No more data to fetch
                                more_data = False
                            else:
                                # Add this batch to our collection
                                all_data.extend(data['data'])
                                pbar.update(batch_size)
                                
                                # Update offset for next batch
                                params['offset'] += batch_size
                                
                                # If we got fewer records than we asked for, we're done
                                if batch_size < params['limit']:
                                    more_data = False
                        else:
                            logger.warning(f"Unexpected data format in API response: {data.keys()}")
                            more_data = False
                            
                    except json.JSONDecodeError:
                        logger.error("Failed to parse JSON response")
                        more_data = False
                else:
                    logger.warning(f"API request failed with status code {response.status_code}.")
                    logger.debug(f"Response content: {response.text[:500]}...")
                    more_data = False
        
        # Convert the collected data to a DataFrame
        if all_data:
            df = pd.DataFrame(all_data)
            logger.info(f"Successfully retrieved {len(df)} records for {year} via API.")
            return df
        else:
            logger.warning(f"No data retrieved for {year} via API.")
            return None
            
    except Exception as e:
        logger.error(f"Error fetching data via API for {year}: {e}")
        return None

def download_bulk_data_direct(year):
    """
    Download SOD data from the FDIC's bulk download page.
    This is a more reliable method for getting complete datasets.
    
    Args:
        year (int): The year to download data for
        
    Returns:
        str: Path to the downloaded file, or None if download failed
    """
    logger.info(f"Attempting bulk download for year {year}...")
    
    # Try multiple potential URL patterns for bulk downloads
    urls_to_try = [
        f"https://www7.fdic.gov/sod/download/SOD_{year}.zip",
        f"https://www7.fdic.gov/sod/sodmarket/SOD_{year}.zip",
        f"https://www5.fdic.gov/sod/download/SOD_{year}.zip",
        f"https://www5.fdic.gov/Historical/SOD/odf{year}.zip"  # Historical format
    ]
    
    for url in urls_to_try:
        try:
            logger.info(f"Trying download from: {url}")
            response = requests.get(url, stream=True)
            
            if response.status_code == 200 and (
                'application/zip' in response.headers.get('Content-Type', '') or
                'application/octet-stream' in response.headers.get('Content-Type', '') or
                'application/x-zip-compressed' in response.headers.get('Content-Type', '')
            ):
                # Save the zip file first
                zip_path = os.path.join(OUTPUT_DIR, f"SOD_{year}.zip")
                with open(zip_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
                
                # Then extract the main CSV file
                with zipfile.ZipFile(zip_path) as z:
                    # List all files in the zip
                    all_files = z.namelist()
                    logger.info(f"Files in zip: {all_files}")
                    
                    # Find the main CSV file - look for patterns
                    csv_candidates = [
                        f for f in all_files 
                        if f.lower().endswith('.csv') and (
                            f.lower().startswith('all_') or  # Common naming pattern
                            'all' in f.lower() or
                            'branch' in f.lower() or
                            'sod' in f.lower()
                        ) and not any(exclude in f.lower() for exclude in ['attr', 'supp', 'readme'])
                    ]
                    
                    if not csv_candidates:
                        # If no obvious main file, just take all CSVs
                        csv_candidates = [f for f in all_files if f.lower().endswith('.csv')]
                    
                    if csv_candidates:
                        # Sort by file size (largest first) as the main file is typically largest
                        csv_candidates.sort(key=lambda x: z.getinfo(x).file_size, reverse=True)
                        
                        csv_file = csv_candidates[0]
                        output_file = os.path.join(OUTPUT_DIR, f"sod_data_{year}.csv")
                        
                        # Extract and save the CSV
                        with z.open(csv_file) as zf, open(output_file, 'wb') as f:
                            f.write(zf.read())
                        
                        logger.info(f"Successfully extracted {csv_file} for {year} from {url}.")
                        
                        # Verify file has adequate records
                        try:
                            row_count = sum(1 for _ in open(output_file)) - 1  # Subtract header
                            logger.info(f"Extracted file contains {row_count} records.")
                            
                            if row_count < 100:  # Sanity check
                                logger.warning(f"File contains suspiciously few records: {row_count}")
                            
                            return output_file
                        except Exception as e:
                            logger.error(f"Error counting rows: {e}")
                            return output_file
                    else:
                        logger.warning(f"No suitable CSV files found in the zip from {url}")
                
            else:
                logger.warning(f"Direct download failed from {url} with status code {response.status_code}")
        
        except Exception as e:
            logger.warning(f"Error trying {url}: {e}")
    
    # If we get here, try the web form download page as last resort
    try:
        logger.info("Attempting to access the FDIC download form page...")
        # Note: This would require more complex web scraping which is beyond our scope
        # We'll just log this as a fallback suggestion
        logger.info("Direct bulk download failed. Manual download may be required from:")
        logger.info("https://www7.fdic.gov/sod/sodMarket.asp?barItem=3")
    except Exception as e:
        logger.error(f"Error with fallback suggestion: {e}")
    
    logger.error(f"All download attempts failed for year {year}")
    return None

def create_data_dictionary():
    """
    Create a data dictionary CSV file that describes all the fields
    in the FDIC Summary of Deposits data.
    
    Based on official FDIC documentation.
    """
    logger.info("Creating data dictionary...")
    
    # Based on SOD documentation and field analysis
    field_descriptions = [
        {"Field": "YEAR", "Description": "Year of the survey (as of June 30)", "Type": "integer"},
        {"Field": "CERT", "Description": "FDIC Certificate Number - unique ID assigned by FDIC", "Type": "integer"},
        {"Field": "DOCKET", "Description": "OTS Docket Number (for thrifts)", "Type": "string"},
        {"Field": "BRNUM", "Description": "Branch Number - unique ID for the branch within the institution", "Type": "integer"},
        {"Field": "UNINUMBR", "Description": "Unique Branch Number - consistent identifier for a physical branch location regardless of ownership", "Type": "integer"},
        {"Field": "NAMEFULL", "Description": "Full legal name of the institution", "Type": "string"},
        {"Field": "NAMEHCR", "Description": "Name of the bank's holding company", "Type": "string"},
        {"Field": "NAMEBR", "Description": "Name of the branch", "Type": "string"},
        {"Field": "ADDRESBR", "Description": "Street address of the branch", "Type": "string"},
        {"Field": "ADDRESS", "Description": "Standardized street address of the branch", "Type": "string"},
        {"Field": "CITYBR", "Description": "City location of the branch", "Type": "string"},
        {"Field": "STALPBR", "Description": "State abbreviation of the branch location", "Type": "string"},
        {"Field": "ZIPBR", "Description": "ZIP code of the branch location", "Type": "string"},
        {"Field": "CNTYNAMB", "Description": "County name where the branch is located", "Type": "string"},
        {"Field": "STCNTYBR", "Description": "FIPS State and County code where the branch is located", "Type": "string"},
        {"Field": "CSABR", "Description": "Combined Statistical Area (CSA) code for the branch", "Type": "string"},
        {"Field": "CBSA", "Description": "Core Based Statistical Area code", "Type": "string"},
        {"Field": "CBSA_DIV_FLG", "Description": "CBSA Division Flag - indicates if MSA has divisions", "Type": "string"},
        {"Field": "MSABR", "Description": "Metropolitan Statistical Area (MSA) code for the branch", "Type": "string"},
        {"Field": "MSANAMB", "Description": "Metropolitan Statistical Area (MSA) name for the branch", "Type": "string"},
        {"Field": "SIMS_LATITUDE", "Description": "Latitude of the branch location (geocoded by FDIC)", "Type": "float"},
        {"Field": "SIMS_LONGITUDE", "Description": "Longitude of the branch location (geocoded by FDIC)", "Type": "float"},
        {"Field": "DEPSUMBR", "Description": "Total deposits held at this branch location", "Type": "float"},
        {"Field": "DEPDOM", "Description": "Total domestic deposits for the institution", "Type": "float"},
        {"Field": "BKMO", "Description": "Branch is the main office (1=Yes, 0=No)", "Type": "integer"},
        {"Field": "BRSERTYP", "Description": "Branch service type (1=Full Service, 2=Limited Service, etc.)", "Type": "string"},
        {"Field": "BRCENM", "Description": "Branch Unique Identifier in Census Mapping", "Type": "string"},
        {"Field": "BKCLASS", "Description": "Bank Class Code", "Type": "string"},
        {"Field": "CB", "Description": "Commercial Bank Indicator", "Type": "string"},
        {"Field": "ASSET", "Description": "Total assets of the institution", "Type": "float"},
        {"Field": "CALL", "Description": "Call Report Identifier", "Type": "string"},
        {"Field": "SIMS_ESTABLISHED_DATE", "Description": "Date the branch was established", "Type": "date"},
        {"Field": "SIMS_ACQUIRED_DATE", "Description": "Date the branch was acquired (if applicable)", "Type": "date"},
        {"Field": "RSSDHCR", "Description": "Regulatory ID for the holding company", "Type": "integer"},
        {"Field": "RSSDID", "Description": "RSSD ID - Federal Reserve identifier for the institution", "Type": "integer"},
        {"Field": "SIMS_PROJECTION", "Description": "Projection system used for geocoding", "Type": "string"},
    ]
    
    # Save data dictionary to CSV
    dict_file = os.path.join(OUTPUT_DIR, DATA_DICTIONARY_FILE)
    with open(dict_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["Field", "Description", "Type"])
        writer.writeheader()
        for row in field_descriptions:
            writer.writerow(row)
    
    logger.info(f"Data dictionary created at {dict_file}")
    return dict_file

def analyze_data_structure(file_path):
    """
    Analyze the first data file to understand the structure and update the data dictionary if needed.
    This helps ensure we have all fields documented.
    """
    try:
        # Read just the header row to get column names
        df = pd.read_csv(file_path, nrows=0)
        columns = df.columns.tolist()
        
        # Check which fields are in our data dictionary
        dict_file = os.path.join(OUTPUT_DIR, DATA_DICTIONARY_FILE)
        existing_dict = pd.read_csv(dict_file)
        existing_fields = existing_dict['Field'].tolist()
        
        # Find new fields
        new_fields = [col for col in columns if col not in existing_fields]
        
        if new_fields:
            logger.info(f"Found {len(new_fields)} new fields not in data dictionary: {', '.join(new_fields)}")
            
            # Append new fields to data dictionary with placeholder descriptions
            with open(dict_file, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=["Field", "Description", "Type"])
                for field in new_fields:
                    writer.writerow({
                        "Field": field,
                        "Description": "Field description not available - update manually",
                        "Type": "unknown"
                    })
            
            logger.info("Updated data dictionary with new fields (placeholder descriptions)")
        
        return True
    except Exception as e:
        logger.error(f"Error analyzing data structure: {e}")
        return False

def main():
    args = parse_args()
    api_key = setup_environment(args.api_key)
    
    logger.info("Starting FDIC Summary of Deposits data collection")
    
    # Determine which years to download
    years_to_download = []
    if args.years:
        try:
            years_to_download = [int(y.strip()) for y in args.years.split(',')]
        except ValueError:
            logger.error("Invalid year format. Use comma-separated years like: 2019,2020,2021")
            sys.exit(1)
    else:
        years_to_download = list(range(START_YEAR, END_YEAR + 1))
    
    # Create data dictionary first
    create_data_dictionary()
    
    # Download data for each year
    successful_downloads = 0
    first_successful_file = None
    
    for year in years_to_download:
        logger.info(f"\nProcessing data for year {year}")
        
        # Approach 1: Try API method first
        df = get_all_sod_data_for_year(year, api_key)
        output_file = None
        
        if df is not None and len(df) > 100:  # Sanity check for adequate records
            # Save the data to CSV
            output_file = os.path.join(OUTPUT_DIR, f"sod_data_{year}.csv")
            df.to_csv(output_file, index=False)
            logger.info(f"Saved {len(df)} records to {output_file}")
        else:
            # Approach 2: Fall back to direct bulk download
            logger.info(f"API method failed or returned insufficient data. Trying bulk download...")
            output_file = download_bulk_data_direct(year)
        
        if output_file:
            if not first_successful_file:
                first_successful_file = output_file
            successful_downloads += 1
    
    # After downloading all years, analyze the structure of the first successful file
    if first_successful_file:
        analyze_data_structure(first_successful_file)
    
    logger.info(f"\nFDIC Summary of Deposits data collection complete.")
    logger.info(f"Successfully downloaded data for {successful_downloads} out of {len(years_to_download)} years.")
    logger.info(f"Data files are stored in the {OUTPUT_DIR} directory.")
    logger.info(f"Data dictionary is available at {os.path.join(OUTPUT_DIR, DATA_DICTIONARY_FILE)}")

if __name__ == "__main__":
    main()