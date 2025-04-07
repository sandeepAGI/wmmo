#!/usr/bin/env python3
"""
census_cbsa_crosswalk.py

This script downloads the Census Bureau's county-to-CBSA crosswalk file
and processes it to create a clean crosswalk mapping counties to Metropolitan
Statistical Areas (MSAs). This is a critical component for aggregating county-level
data to MSA level for the Wealth Management Market Opportunity analysis.
"""

import os
import requests
import pandas as pd
import zipfile
import io
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("census_cbsa_crosswalk.log"),
        logging.StreamHandler()
    ]
)

# Constants
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
DELINEATION_URL = "https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2020/delineation-files/list1_2020.xls"
COUNTY_CROSSWALK_URL = "https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2020/delineation-files/list3_2020.xls"

def download_file(url, output_path):
    """
    Download a file from the specified URL and save it to the output path.
    
    Args:
        url (str): URL of the file to download
        output_path (str): Path where the file should be saved
    
    Returns:
        bool: True if download was successful, False otherwise
    """
    logging.info(f"Downloading {url}...")
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        with open(output_path, 'wb') as f:
            f.write(response.content)
        
        logging.info(f"Downloaded successfully to {output_path}")
        return True
    except Exception as e:
        logging.error(f"Error downloading {url}: {e}")
        return False

def process_delineation_file(file_path):
    """
    Process the main CBSA delineation file to extract MSA/CBSA information.
    
    Args:
        file_path (str): Path to the delineation file
    
    Returns:
        pandas.DataFrame: DataFrame containing CBSA codes and names
    """
    logging.info(f"Processing CBSA delineation file: {file_path}")
    try:
        # Read the Excel file
        df = pd.read_excel(file_path, skiprows=2)
        
        # Clean up column names
        df.columns = [col.strip() for col in df.columns]
        
        # Select only needed columns and filter for Metropolitan Statistical Areas (not Micropolitan)
        cbsa_info = df[['CBSA Code', 'CBSA Title', 'Metropolitan/Micropolitan Statistical Area']]
        cbsa_info = cbsa_info[cbsa_info['Metropolitan/Micropolitan Statistical Area'] == 'Metropolitan Statistical Area'].copy()
        
        # Rename columns for clarity
        cbsa_info.rename(columns={
            'CBSA Code': 'cbsa_code',
            'CBSA Title': 'cbsa_title',
            'Metropolitan/Micropolitan Statistical Area': 'area_type'
        }, inplace=True)
        
        logging.info(f"Extracted information for {len(cbsa_info)} Metropolitan Statistical Areas")
        return cbsa_info
    except Exception as e:
        logging.error(f"Error processing delineation file: {e}")
        return None

def process_county_crosswalk(file_path):
    """
    Process the county-to-CBSA crosswalk file to create a mapping of counties to CBSAs.
    
    Args:
        file_path (str): Path to the county crosswalk file
    
    Returns:
        pandas.DataFrame: DataFrame containing county to CBSA mappings
    """
    logging.info(f"Processing county-to-CBSA crosswalk file: {file_path}")
    try:
        # Read the Excel file
        df = pd.read_excel(file_path, skiprows=2)
        
        # Clean up column names
        df.columns = [col.strip() for col in df.columns]
        
        # Select only needed columns
        county_crosswalk = df[[
            'FIPS State Code', 
            'FIPS County Code', 
            'County/County Equivalent',
            'CBSA Code', 
            'CBSA Title',
            'Metropolitan/Micropolitan Statistical Area',
            'Metropolitan Division Title',
            'CSA Code',
            'CSA Title'
        ]].copy()
        
        # Create FIPS codes as strings with leading zeros
        county_crosswalk['state_fips'] = county_crosswalk['FIPS State Code'].astype(str).str.zfill(2)
        county_crosswalk['county_fips'] = county_crosswalk['FIPS County Code'].astype(str).str.zfill(3)
        county_crosswalk['fips_code'] = county_crosswalk['state_fips'] + county_crosswalk['county_fips']
        
        # Rename columns for clarity
        county_crosswalk.rename(columns={
            'FIPS State Code': 'fips_state_code',
            'FIPS County Code': 'fips_county_code',
            'County/County Equivalent': 'county_name',
            'CBSA Code': 'cbsa_code',
            'CBSA Title': 'cbsa_title',
            'Metropolitan/Micropolitan Statistical Area': 'area_type',
            'Metropolitan Division Title': 'metro_division_title',
            'CSA Code': 'csa_code',
            'CSA Title': 'csa_title'
        }, inplace=True)
        
        # Filter for only Metropolitan areas (not Micropolitan)
        metro_counties = county_crosswalk[county_crosswalk['area_type'] == 'Metropolitan Statistical Area'].copy()
        
        logging.info(f"Processed crosswalk with {len(metro_counties)} counties in Metropolitan Statistical Areas")
        return metro_counties
    except Exception as e:
        logging.error(f"Error processing county crosswalk file: {e}")
        return None

def create_additional_mappings(metro_counties):
    """
    Create additional useful mappings from the county-to-CBSA data.
    
    Args:
        metro_counties (pandas.DataFrame): DataFrame with county-to-CBSA mappings
    
    Returns:
        dict: Dictionary containing additional mappings
    """
    logging.info("Creating additional mappings...")
    try:
        # Create map of CBSA codes to titles
        cbsa_to_title = metro_counties[['cbsa_code', 'cbsa_title']].drop_duplicates().set_index('cbsa_code')['cbsa_title'].to_dict()
        
        # Group counties by CBSA
        cbsa_counties = {}
        for cbsa_code, group in metro_counties.groupby('cbsa_code'):
            cbsa_counties[cbsa_code] = group['fips_code'].tolist()
        
        # Create reverse mapping from county FIPS to CBSA
        county_to_cbsa = metro_counties[['fips_code', 'cbsa_code']].set_index('fips_code')['cbsa_code'].to_dict()
        
        # Additional state-level information
        states_in_cbsa = {}
        for cbsa_code, group in metro_counties.groupby('cbsa_code'):
            states_in_cbsa[cbsa_code] = group['state_fips'].unique().tolist()
        
        # Create dictionary with all mappings
        mappings = {
            'cbsa_to_title': cbsa_to_title,
            'cbsa_counties': cbsa_counties,
            'county_to_cbsa': county_to_cbsa,
            'states_in_cbsa': states_in_cbsa
        }
        
        logging.info("Successfully created additional mappings")
        return mappings
    except Exception as e:
        logging.error(f"Error creating additional mappings: {e}")
        return None

def main():
    """Main function to download and process crosswalk files"""
    # Create timestamp for filenames
    timestamp = datetime.now().strftime("%Y%m%d")
    
    # Download delineation file
    delineation_file = os.path.join(OUTPUT_DIR, f"cbsa_delineation_{timestamp}.xls")
    if not download_file(DELINEATION_URL, delineation_file):
        return
    
    # Download county crosswalk
    crosswalk_file = os.path.join(OUTPUT_DIR, f"county_cbsa_crosswalk_{timestamp}.xls")
    if not download_file(COUNTY_CROSSWALK_URL, crosswalk_file):
        return
    
    # Process CBSA delineation file
    cbsa_info = process_delineation_file(delineation_file)
    if cbsa_info is None:
        return
    
    # Process county crosswalk file
    metro_counties = process_county_crosswalk(crosswalk_file)
    if metro_counties is None:
        return
    
    # Create additional mappings
    mappings = create_additional_mappings(metro_counties)
    if mappings is None:
        return
    
    # Save processed files
    cbsa_info_file = os.path.join(OUTPUT_DIR, f"cbsa_info_{timestamp}.csv")
    cbsa_info.to_csv(cbsa_info_file, index=False)
    logging.info(f"Saved CBSA information to {cbsa_info_file}")
    
    metro_counties_file = os.path.join(OUTPUT_DIR, f"metro_counties_{timestamp}.csv")
    metro_counties.to_csv(metro_counties_file, index=False)
    logging.info(f"Saved metro counties to {metro_counties_file}")
    
    # Save mappings to JSON
    import json
    mappings_file = os.path.join(OUTPUT_DIR, f"cbsa_mappings_{timestamp}.json")
    with open(mappings_file, 'w') as f:
        json.dump(mappings, f, indent=4)
    logging.info(f"Saved CBSA mappings to {mappings_file}")
    
    # Create a more convenient pickle file that combines everything
    import pickle
    combined_data = {
        'cbsa_info': cbsa_info,
        'metro_counties': metro_counties,
        'mappings': mappings
    }
    pickle_file = os.path.join(OUTPUT_DIR, f"cbsa_crosswalk_data_{timestamp}.pkl")
    with open(pickle_file, 'wb') as f:
        pickle.dump(combined_data, f)
    logging.info(f"Saved combined crosswalk data to {pickle_file}")
    
    # Create crosswalk readme
    readme_content = f"""# Census CBSA-to-County Crosswalk

This directory contains crosswalk files mapping counties to Core-Based Statistical Areas (CBSAs), 
which include Metropolitan and Micropolitan Statistical Areas. These files were downloaded from 
the U.S. Census Bureau on {timestamp}.

## Files

- `cbsa_delineation_{timestamp}.xls`: Original delineation file from Census Bureau
- `county_cbsa_crosswalk_{timestamp}.xls`: Original county-to-CBSA crosswalk from Census Bureau
- `cbsa_info_{timestamp}.csv`: Processed file containing information about CBSAs
- `metro_counties_{timestamp}.csv`: Processed file containing county-to-CBSA mappings (metropolitan areas only)
- `cbsa_mappings_{timestamp}.json`: JSON file with useful mappings (CBSA to title, county to CBSA, etc.)
- `cbsa_crosswalk_data_{timestamp}.pkl`: Python pickle file containing all crosswalk data

## Data Sources

- CBSA Delineation File: {DELINEATION_URL}
- County-to-CBSA Crosswalk: {COUNTY_CROSSWALK_URL}

These files are based on the 2020 CBSA delineations by the Office of Management and Budget.

## Usage

The pickle file contains a dictionary with the following keys:

- `cbsa_info`: DataFrame with information about CBSAs
- `metro_counties`: DataFrame with county-to-CBSA mappings
- `mappings`: Dictionary with useful mappings:
  - `cbsa_to_title`: CBSA code to CBSA title
  - `cbsa_counties`: CBSA code to list of county FIPS codes
  - `county_to_cbsa`: County FIPS code to CBSA code
  - `states_in_cbsa`: CBSA code to list of state FIPS codes

To load the pickle file:

```python
import pickle
with open('cbsa_crosswalk_data_{timestamp}.pkl', 'rb') as f:
    crosswalk_data = pickle.load(f)
```

"""
    
    readme_file = os.path.join(OUTPUT_DIR, "README.md")
    with open(readme_file, 'w') as f:
        f.write(readme_content)
    logging.info(f"Created README file at {readme_file}")
    
    logging.info("CBSA crosswalk processing complete!")

if __name__ == "__main__":
    main()