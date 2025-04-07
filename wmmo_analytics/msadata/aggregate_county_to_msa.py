#!/usr/bin/env python3
"""
aggregate_county_to_msa.py

This script aggregates county-level data from various sources (BEA, Census ACS, IRS SOI)
to Metropolitan Statistical Area (MSA) level using the Census CBSA crosswalk.
The aggregated data is saved in the msadata directory for further analysis.
"""

import os
import sys
import pandas as pd
import numpy as np
import pickle
import glob
import json
import logging
from datetime import datetime
from pathlib import Path

# Add parent directory to path to import modules from other directories
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("aggregate_county_to_msa.log"),
        logging.StreamHandler()
    ]
)

# Constants
CROSSWALK_DIR = os.path.join(parent_dir, "crosswalks")
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
BEA_DATA_DIR = os.path.join(parent_dir, "..", "bea_data")
CENSUS_DATA_DIR = os.path.join(parent_dir, "..", "census_acs_data")
IRS_DATA_DIR = os.path.join(parent_dir, "..", "irs_soi_data")
FDIC_DATA_DIR = os.path.join(parent_dir, "..", "fdic_deposit_data")
BLS_DATA_DIR = os.path.join(parent_dir, "..", "bls_advisors_data")

def load_crosswalk():
    """
    Load the most recent CBSA crosswalk data.
    
    Returns:
        dict: Dictionary containing crosswalk data
    """
    logging.info("Loading CBSA crosswalk data...")
    try:
        # Find the most recent crosswalk pickle file
        pickle_files = glob.glob(os.path.join(CROSSWALK_DIR, "cbsa_crosswalk_data_*.pkl"))
        if not pickle_files:
            logging.error("No crosswalk pickle files found. Run census_cbsa_crosswalk.py first.")
            return None
        
        # Sort by filename (which includes timestamp) to get the most recent
        most_recent = sorted(pickle_files)[-1]
        logging.info(f"Using crosswalk file: {most_recent}")
        
        # Load the pickle file
        with open(most_recent, 'rb') as f:
            crosswalk_data = pickle.load(f)
        
        logging.info(f"Loaded crosswalk data with {len(crosswalk_data['cbsa_info'])} CBSAs and {len(crosswalk_data['metro_counties'])} counties")
        return crosswalk_data
    
    except Exception as e:
        logging.error(f"Error loading crosswalk data: {e}")
        return None

def load_bea_data():
    """
    Load BEA data files and prepare them for aggregation.
    
    Returns:
        dict: Dictionary of DataFrames containing BEA data
    """
    logging.info("Loading BEA data...")
    try:
        bea_data = {}
        
        # Load GDP data
        gdp_files = glob.glob(os.path.join(BEA_DATA_DIR, "county_gdp_*.csv"))
        if gdp_files:
            most_recent = sorted(gdp_files)[-1]
            logging.info(f"Loading GDP data from: {most_recent}")
            gdp_df = pd.read_csv(most_recent)
            
            # Clean and prepare data
            gdp_df['GeoFips'] = gdp_df['GeoFips'].astype(str).str.zfill(5)
            gdp_df['DataValue'] = pd.to_numeric(gdp_df['DataValue'], errors='coerce')
            gdp_df['TimePeriod'] = pd.to_numeric(gdp_df['TimePeriod'], errors='coerce')
            
            # Remove USA, state-level, and MSA-level entries (keep only counties)
            gdp_df = gdp_df[gdp_df['GeoFips'].str.len() == 5]
            gdp_df = gdp_df[~gdp_df['GeoFips'].str.endswith('000')]  # Remove state-level entries
            
            bea_data['gdp'] = gdp_df
        
        # Load Personal Income data
        income_files = glob.glob(os.path.join(BEA_DATA_DIR, "county_personal_income_*.csv"))
        if income_files:
            most_recent = sorted(income_files)[-1]
            logging.info(f"Loading Personal Income data from: {most_recent}")
            income_df = pd.read_csv(most_recent)
            
            # Clean and prepare data
            income_df['GeoFips'] = income_df['GeoFips'].astype(str).str.zfill(5)
            income_df['DataValue'] = pd.to_numeric(income_df['DataValue'], errors='coerce')
            income_df['TimePeriod'] = pd.to_numeric(income_df['TimePeriod'], errors='coerce')
            
            # Remove USA, state-level, and MSA-level entries (keep only counties)
            income_df = income_df[income_df['GeoFips'].str.len() == 5]
            income_df = income_df[~income_df['GeoFips'].str.endswith('000')]  # Remove state-level entries
            
            bea_data['personal_income'] = income_df
        
        # Load Population data
        pop_files = glob.glob(os.path.join(BEA_DATA_DIR, "county_population_*.csv"))
        if pop_files:
            most_recent = sorted(pop_files)[-1]
            logging.info(f"Loading Population data from: {most_recent}")
            pop_df = pd.read_csv(most_recent)
            
            # Clean and prepare data
            pop_df['GeoFips'] = pop_df['GeoFips'].astype(str).str.zfill(5)
            pop_df['DataValue'] = pd.to_numeric(pop_df['DataValue'], errors='coerce')
            pop_df['TimePeriod'] = pd.to_numeric(pop_df['TimePeriod'], errors='coerce')
            
            # Remove USA, state-level, and MSA-level entries (keep only counties)
            pop_df = pop_df[pop_df['GeoFips'].str.len() == 5]
            pop_df = pop_df[~pop_df['GeoFips'].str.endswith('000')]  # Remove state-level entries
            
            bea_data['population'] = pop_df
        
        # Load Per Capita Income data
        pci_files = glob.glob(os.path.join(BEA_DATA_DIR, "county_per_capita_income_*.csv"))
        if pci_files:
            most_recent = sorted(pci_files)[-1]
            logging.info(f"Loading Per Capita Income data from: {most_recent}")
            pci_df = pd.read_csv(most_recent)
            
            # Clean and prepare data
            pci_df['GeoFips'] = pci_df['GeoFips'].astype(str).str.zfill(5)
            pci_df['DataValue'] = pd.to_numeric(pci_df['DataValue'], errors='coerce')
            pci_df['TimePeriod'] = pd.to_numeric(pci_df['TimePeriod'], errors='coerce')
            
            # Remove USA, state-level, and MSA-level entries (keep only counties)
            pci_df = pci_df[pci_df['GeoFips'].str.len() == 5]
            pci_df = pci_df[~pci_df['GeoFips'].str.endswith('000')]  # Remove state-level entries
            
            bea_data['per_capita_income'] = pci_df
            
        # Load Industry Income data
        ind_files = glob.glob(os.path.join(BEA_DATA_DIR, "county_income_by_industry_*.csv"))
        if ind_files:
            most_recent = sorted(ind_files)[-1]
            logging.info(f"Loading Industry Income data from: {most_recent}")
            ind_df = pd.read_csv(most_recent)
            
            # Clean and prepare data
            ind_df['GeoFips'] = ind_df['GeoFips'].astype(str).str.zfill(5)
            ind_df['DataValue'] = pd.to_numeric(ind_df['DataValue'], errors='coerce')
            ind_df['TimePeriod'] = pd.to_numeric(ind_df['TimePeriod'], errors='coerce')
            
            # Remove USA, state-level, and MSA-level entries (keep only counties)
            ind_df = ind_df[ind_df['GeoFips'].str.len() == 5]
            ind_df = ind_df[~ind_df['GeoFips'].str.endswith('000')]  # Remove state-level entries
            
            bea_data['income_by_industry'] = ind_df
        
        logging.info(f"Loaded BEA data: {', '.join(bea_data.keys())}")
        return bea_data
    
    except Exception as e:
        logging.error(f"Error loading BEA data: {e}")
        return {}

def load_census_acs_data():
    """
    Load Census ACS data files and prepare them for aggregation.
    
    Returns:
        dict: Dictionary containing Census ACS data
    """
    logging.info("Loading Census ACS data...")
    try:
        census_data = {}
        
        # Find the most recent ACS data file
        acs_files = glob.glob(os.path.join(CENSUS_DATA_DIR, "census_acs_county_data_*.csv"))
        if not acs_files:
            logging.warning("No Census ACS data files found.")
            return census_data
        
        most_recent = sorted(acs_files)[-1]
        logging.info(f"Loading Census ACS data from: {most_recent}")
        
        # Load the data
        acs_df = pd.read_csv(most_recent)
        
        # Clean and prepare data
        # Create FIPS code from state and county codes
        acs_df['fips_code'] = acs_df['state'].astype(str).str.zfill(2) + acs_df['county'].astype(str).str.zfill(3)
        
        # Convert data columns to numeric
        for col in acs_df.columns:
            if col.startswith('B') and col.endswith('E'):  # These are the ACS data columns
                acs_df[col] = pd.to_numeric(acs_df[col], errors='coerce')
        
        census_data['acs'] = acs_df
        logging.info(f"Loaded Census ACS data with {len(acs_df)} counties")
        
        # Load data dictionary if available
        dict_files = glob.glob(os.path.join(CENSUS_DATA_DIR, "census_acs_data_dictionary_*.csv"))
        if dict_files:
            dict_file = sorted(dict_files)[-1]
            census_data['data_dictionary'] = pd.read_csv(dict_file)
            logging.info(f"Loaded Census ACS data dictionary with {len(census_data['data_dictionary'])} variables")
        
        return census_data
    
    except Exception as e:
        logging.error(f"Error loading Census ACS data: {e}")
        return {}

def load_irs_soi_data():
    """
    Load IRS SOI data and prepare it for aggregation.
    
    Returns:
        dict: Dictionary containing IRS SOI data
    """
    logging.info("Loading IRS SOI data...")
    try:
        irs_data = {}
        
        # Check for the combined file first
        combined_file = os.path.join(IRS_DATA_DIR, "irs_soi_zipcode_all_years.csv")
        if os.path.exists(combined_file):
            logging.info(f"Loading combined IRS SOI data from: {combined_file}")
            irs_df = pd.read_csv(combined_file)
            irs_data['combined'] = irs_df
        else:
            # If no combined file, look for individual year files
            year_dirs = glob.glob(os.path.join(IRS_DATA_DIR, "tax_year_*"))
            for year_dir in sorted(year_dirs):
                year = os.path.basename(year_dir).replace("tax_year_", "")
                processed_files = glob.glob(os.path.join(year_dir, "*processed.csv"))
                if processed_files:
                    processed_file = processed_files[0]
                    logging.info(f"Loading IRS SOI data for {year} from: {processed_file}")
                    year_df = pd.read_csv(processed_file)
                    irs_data[year] = year_df
        
        # Load data dictionary if available
        dict_file = os.path.join(IRS_DATA_DIR, "irs_soi_data_dictionary.csv")
        if os.path.exists(dict_file):
            irs_data['data_dictionary'] = pd.read_csv(dict_file)
            logging.info(f"Loaded IRS SOI data dictionary with {len(irs_data['data_dictionary'])} fields")
        
        logging.info(f"Loaded IRS SOI data for {len([k for k in irs_data.keys() if k != 'data_dictionary'])} datasets")
        return irs_data
    
    except Exception as e:
        logging.error(f"Error loading IRS SOI data: {e}")
        return {}

def load_fdic_deposit_data():
    """
    Load FDIC deposit data and prepare it for analysis.
    
    Returns:
        dict: Dictionary containing FDIC deposit data
    """
    logging.info("Loading FDIC deposit data...")
    try:
        fdic_data = {}
        
        # Find all data files
        deposit_files = glob.glob(os.path.join(FDIC_DATA_DIR, "sod_data_*.csv"))
        for file_path in sorted(deposit_files):
            year = os.path.basename(file_path).replace("sod_data_", "").replace(".csv", "")
            logging.info(f"Loading FDIC deposit data for {year} from: {file_path}")
            df = pd.read_csv(file_path, low_memory=False)
            
            # Clean up data
            if 'STALPBR' in df.columns and 'ZIPBR' in df.columns:
                # Ensure ZIP code is properly formatted
                df['ZIPBR'] = df['ZIPBR'].astype(str).str[:5]
                fdic_data[year] = df
        
        # Load data dictionary if available
        dict_file = os.path.join(FDIC_DATA_DIR, "fdic_sod_data_dictionary.csv")
        if os.path.exists(dict_file):
            fdic_data['data_dictionary'] = pd.read_csv(dict_file)
            logging.info(f"Loaded FDIC data dictionary with {len(fdic_data['data_dictionary'])} fields")
        
        logging.info(f"Loaded FDIC deposit data for {len([k for k in fdic_data.keys() if k != 'data_dictionary'])} years")
        return fdic_data
    
    except Exception as e:
        logging.error(f"Error loading FDIC deposit data: {e}")
        return {}

def load_bls_advisor_data():
    """
    Load BLS data on financial advisors and prepare it for analysis.
    
    Returns:
        dict: Dictionary containing BLS advisor data
    """
    logging.info("Loading BLS advisor data...")
    try:
        bls_data = {}
        
        # Find all CSV files in the BLS data directory
        advisor_files = glob.glob(os.path.join(BLS_DATA_DIR, "*.csv"))
        for file_path in sorted(advisor_files):
            year = os.path.basename(file_path).replace(".csv", "")
            if year.isdigit():
                logging.info(f"Loading BLS advisor data for {year} from: {file_path}")
                df = pd.read_csv(file_path)
                
                # Clean and prepare data
                if 'area_title' in df.columns and 'tot_emp' in df.columns:
                    df['tot_emp'] = pd.to_numeric(df['tot_emp'], errors='coerce')
                    bls_data[year] = df
        
        # Load data dictionary if available
        dict_file = os.path.join(BLS_DATA_DIR, "data_dictionary.json")
        if os.path.exists(dict_file):
            with open(dict_file, 'r') as f:
                bls_data['data_dictionary'] = json.load(f)
            logging.info("Loaded BLS advisor data dictionary")
        
        logging.info(f"Loaded BLS advisor data for {len([k for k in bls_data.keys() if k != 'data_dictionary'])} years")
        return bls_data
    
    except Exception as e:
        logging.error(f"Error loading BLS advisor data: {e}")
        return {}

def aggregate_bea_to_msa(bea_data, crosswalk_data):
    """
    Aggregate BEA county-level data to MSA level.
    
    Args:
        bea_data (dict): Dictionary of DataFrames containing BEA data
        crosswalk_data (dict): Dictionary containing crosswalk data
    
    Returns:
        dict: Dictionary of DataFrames with MSA-level BEA data
    """
    logging.info("Aggregating BEA data to MSA level...")
    msa_bea_data = {}
    
    try:
        # Extract mappings
        county_to_cbsa = crosswalk_data['mappings']['county_to_cbsa']
        cbsa_to_title = crosswalk_data['mappings']['cbsa_to_title']
        
        # Process GDP data
        if 'gdp' in bea_data:
            logging.info("Aggregating GDP data...")
            gdp_df = bea_data['gdp'].copy()
            
            # Add CBSA code to each county
            gdp_df['cbsa_code'] = gdp_df['GeoFips'].map(county_to_cbsa)
            
            # Keep only counties that are in MSAs
            gdp_df = gdp_df[~gdp_df['cbsa_code'].isna()].copy()
            
            # Group by CBSA and year and sum GDP values
            msa_gdp = gdp_df.groupby(['cbsa_code', 'TimePeriod']).agg({
                'DataValue': 'sum'
            }).reset_index()
            
            # Add CBSA title
            msa_gdp['cbsa_title'] = msa_gdp['cbsa_code'].map(cbsa_to_title)
            
            msa_bea_data['gdp'] = msa_gdp
            
            # Calculate year-over-year GDP growth
            msa_gdp_growth = msa_gdp.copy()
            msa_gdp_growth = msa_gdp_growth.sort_values(['cbsa_code', 'TimePeriod'])
            msa_gdp_growth['prev_gdp'] = msa_gdp_growth.groupby('cbsa_code')['DataValue'].shift(1)
            msa_gdp_growth['prev_year'] = msa_gdp_growth.groupby('cbsa_code')['TimePeriod'].shift(1)
            msa_gdp_growth['year_diff'] = msa_gdp_growth['TimePeriod'] - msa_gdp_growth['prev_year']
            msa_gdp_growth['gdp_growth'] = (msa_gdp_growth['DataValue'] / msa_gdp_growth['prev_gdp']) - 1
            msa_gdp_growth['annual_gdp_growth'] = np.where(
                msa_gdp_growth['year_diff'] > 1,
                (1 + msa_gdp_growth['gdp_growth'])**(1/msa_gdp_growth['year_diff']) - 1,
                msa_gdp_growth['gdp_growth']
            )
            
            msa_bea_data['gdp_growth'] = msa_gdp_growth
        
        # Process Population data
        if 'population' in bea_data:
            logging.info("Aggregating Population data...")
            pop_df = bea_data['population'].copy()
            
            # Add CBSA code to each county
            pop_df['cbsa_code'] = pop_df['GeoFips'].map(county_to_cbsa)
            
            # Keep only counties that are in MSAs
            pop_df = pop_df[~pop_df['cbsa_code'].isna()].copy()
            
            # Group by CBSA and year and sum population values
            msa_pop = pop_df.groupby(['cbsa_code', 'TimePeriod']).agg({
                'DataValue': 'sum'
            }).reset_index()
            
            # Add CBSA title
            msa_pop['cbsa_title'] = msa_pop['cbsa_code'].map(cbsa_to_title)
            
            msa_bea_data['population'] = msa_pop
        
        # Process Personal Income data
        if 'personal_income' in bea_data:
            logging.info("Aggregating Personal Income data...")
            inc_df = bea_data['personal_income'].copy()
            
            # Add CBSA code to each county
            inc_df['cbsa_code'] = inc_df['GeoFips'].map(county_to_cbsa)
            
            # Keep only counties that are in MSAs
            inc_df = inc_df[~inc_df['cbsa_code'].isna()].copy()
            
            # Group by CBSA and year and sum income values
            msa_inc = inc_df.groupby(['cbsa_code', 'TimePeriod']).agg({
                'DataValue': 'sum'
            }).reset_index()
            
            # Add CBSA title
            msa_inc['cbsa_title'] = msa_inc['cbsa_code'].map(cbsa_to_title)
            
            msa_bea_data['personal_income'] = msa_inc
            
            # Calculate MSA-level per capita income (combine with population data)
            if 'population' in msa_bea_data:
                msa_pci = pd.merge(
                    msa_inc.rename(columns={'DataValue': 'total_income'}),
                    msa_bea_data['population'].rename(columns={'DataValue': 'population'}),
                    on=['cbsa_code', 'TimePeriod', 'cbsa_title']
                )
                
                # Calculate per capita income
                msa_pci['per_capita_income'] = msa_pci['total_income'] * 1000 / msa_pci['population']
                
                msa_bea_data['per_capita_income_calculated'] = msa_pci
        
        # Process industry data if available
        if 'income_by_industry' in bea_data:
            logging.info("Aggregating Income by Industry data...")
            ind_df = bea_data['income_by_industry'].copy()
            
            # Add CBSA code to each county
            ind_df['cbsa_code'] = ind_df['GeoFips'].map(county_to_cbsa)
            
            # Keep only counties that are in MSAs
            ind_df = ind_df[~ind_df['cbsa_code'].isna()].copy()
            
            # Group by CBSA, year, and industry (LineCode) and sum values
            msa_ind = ind_df.groupby(['cbsa_code', 'TimePeriod', 'LineCode', 'Description']).agg({
                'DataValue': 'sum'
            }).reset_index()
            
            # Add CBSA title
            msa_ind['cbsa_title'] = msa_ind['cbsa_code'].map(cbsa_to_title)
            
            msa_bea_data['income_by_industry'] = msa_ind
            
            # Create a pivot table for easier analysis
            msa_ind_pivot = msa_ind.pivot_table(
                index=['cbsa_code', 'cbsa_title', 'TimePeriod'],
                columns='LineCode',
                values='DataValue'
            ).reset_index()
            
            msa_bea_data['income_by_industry_pivot'] = msa_ind_pivot
        
        logging.info(f"BEA data aggregation complete. Created {len(msa_bea_data)} MSA-level datasets.")
        return msa_bea_data
    
    except Exception as e:
        logging.error(f"Error aggregating BEA data: {e}")
        return {}

def aggregate_census_to_msa(census_data, crosswalk_data):
    """
    Aggregate Census ACS county-level data to MSA level.
    
    Args:
        census_data (dict): Dictionary containing Census ACS data
        crosswalk_data (dict): Dictionary containing crosswalk data
    
    Returns:
        dict: Dictionary of DataFrames with MSA-level Census data
    """
    logging.info("Aggregating Census ACS data to MSA level...")
    msa_census_data = {}
    
    try:
        if 'acs' not in census_data:
            logging.warning("No Census ACS data available for aggregation.")
            return msa_census_data
        
        # Extract mappings
        county_to_cbsa = crosswalk_data['mappings']['county_to_cbsa']
        cbsa_to_title = crosswalk_data['mappings']['cbsa_to_title']
        
        # Get the ACS data
        acs_df = census_data['acs'].copy()
        
        # Add CBSA code to each county
        acs_df['cbsa_code'] = acs_df['fips_code'].map(county_to_cbsa)
        
        # Keep only counties that are in MSAs
        acs_df = acs_df[~acs_df['cbsa_code'].isna()].copy()
        
        # Add CBSA title
        acs_df['cbsa_title'] = acs_df['cbsa_code'].map(cbsa_to_title)
        
        # Define countable (summable) variables
        countable_vars = [
            'B01001_001E',  # Total population
            'B01001_014E', 'B01001_015E', 'B01001_016E', 'B01001_017E',  # Males 45-64
            'B01001_038E', 'B01001_039E', 'B01001_040E', 'B01001_041E',  # Females 45-64
            'B19001_001E',  # Total households
            'B19001_014E', 'B19001_015E', 'B19001_016E', 'B19001_017E',  # High-income households
            'B15003_001E',  # Total population 25 years and over
            'B15003_022E', 'B15003_023E', 'B15003_024E', 'B15003_025E',  # College degrees
            'B25075_001E',  # Total owner-occupied housing units
            'B25075_020E', 'B25075_021E', 'B25075_022E', 'B25075_023E', 'B25075_024E',  # High-value homes
        ]
        
        # Define weighted average variables
        weighted_vars = {
            'B19013_001E': 'B19001_001E',  # Median household income weighted by households
            'B19301_001E': 'B01001_001E',  # Per capita income weighted by population
            'B25077_001E': 'B25075_001E',  # Median home value weighted by housing units
            'B01002_001E': 'B01001_001E',  # Median age weighted by population
        }
        
        # Aggregate countable variables
        msa_acs = acs_df.groupby(['cbsa_code', 'cbsa_title', 'year']).agg({
            var: 'sum' for var in countable_vars if var in acs_df.columns
        }).reset_index()
        
        # Handle weighted averages
        for avg_var, weight_var in weighted_vars.items():
            if avg_var in acs_df.columns and weight_var in acs_df.columns:
                # Calculate weighted values at county level
                acs_df[f'{avg_var}_weighted'] = acs_df[avg_var] * acs_df[weight_var]
                
                # Sum weighted values and weights at MSA level
                weighted_sum = acs_df.groupby(['cbsa_code'])[[f'{avg_var}_weighted', weight_var]].sum()
                
                # Calculate weighted average
                weighted_sum[avg_var] = weighted_sum[f'{avg_var}_weighted'] / weighted_sum[weight_var]
                
                # Merge with main MSA data
                msa_acs = pd.merge(
                    msa_acs,
                    weighted_sum[[avg_var]].reset_index(),
                    on='cbsa_code',
                    how='left'
                )
        
        # Calculate derived metrics
        # High-income household percentage
        if all(x in msa_acs.columns for x in ['B19001_017E', 'B19001_001E']):
            msa_acs['high_income_household_pct'] = (msa_acs['B19001_017E'] / msa_acs['B19001_001E']) * 100
        
        # Luxury home percentage
        if all(x in msa_acs.columns for x in ['B25075_022E', 'B25075_023E', 'B25075_024E', 'B25075_001E']):
            msa_acs['luxury_home_count'] = msa_acs['B25075_022E'] + msa_acs['B25075_023E'] + msa_acs['B25075_024E']
            msa_acs['luxury_home_pct'] = (msa_acs['luxury_home_count'] / msa_acs['B25075_001E']) * 100
        
        # College education percentage
        if all(x in msa_acs.columns for x in ['B15003_022E', 'B15003_023E', 'B15003_024E', 'B15003_025E', 'B15003_001E']):
            msa_acs['college_degree_count'] = msa_acs['B15003_022E'] + msa_acs['B15003_023E'] + msa_acs['B15003_024E'] + msa_acs['B15003_025E']
            msa_acs['college_degree_pct'] = (msa_acs['college_degree_count'] / msa_acs['B15003_001E']) * 100
        
        # Prime wealth accumulation age (45-64) percentage
        if all(x in msa_acs.columns for x in ['B01001_014E', 'B01001_015E', 'B01001_016E', 'B01001_017E', 
                                             'B01001_038E', 'B01001_039E', 'B01001_040E', 'B01001_041E', 'B01001_001E']):
            msa_acs['wealth_accum_age_count'] = (
                msa_acs['B01001_014E'] + msa_acs['B01001_015E'] + msa_acs['B01001_016E'] + msa_acs['B01001_017E'] +
                msa_acs['B01001_038E'] + msa_acs['B01001_039E'] + msa_acs['B01001_040E'] + msa_acs['B01001_041E']
            )
            msa_acs['wealth_accum_age_pct'] = (msa_acs['wealth_accum_age_count'] / msa_acs['B01001_001E']) * 100
        
        msa_census_data['acs'] = msa_acs
        logging.info(f"Census ACS data aggregation complete with {len(msa_acs)} MSAs")
        
        return msa_census_data
    
    except Exception as e:
        logging.error(f"Error aggregating Census data: {e}")
        return {}

def aggregate_irs_to_msa(irs_data, crosswalk_data):
    """
    Aggregate IRS SOI data to MSA level.
    
    Args:
        irs_data (dict): Dictionary containing IRS SOI data
        crosswalk_data (dict): Dictionary containing crosswalk data
    
    Returns:
        dict: Dictionary of DataFrames with MSA-level IRS data
    """
    logging.info("Aggregating IRS SOI data to MSA level...")
    msa_irs_data = {}
    
    try:
        # Check if data is available
        if 'combined' not in irs_data and not any(k.isdigit() for k in irs_data.keys()):
            logging.warning("No IRS SOI data available for aggregation.")
            return msa_irs_data
        
        # Extract mappings
        county_to_cbsa = crosswalk_data['mappings']['county_to_cbsa']
        cbsa_to_title = crosswalk_data['mappings']['cbsa_to_title']
        
        # Process ZIP code data - this requires a crosswalk from ZIP to county
        # Since we don't have that directly in the source data, we'll need to approximate
        
        # First, check if we have the combined data
        if 'combined' in irs_data:
            irs_df = irs_data['combined'].copy()
            
            # Simplify by focusing on high-income households (AGI_STUB=6)
            high_income_df = irs_df[irs_df['AGI_STUB'] == 6].copy()
            
            # We need to map ZIP codes to counties, which we don't have directly
            # For this POC, we'll use the first 3 digits of ZIP code to approximate county
            # In a production system, a proper ZIP-to-county crosswalk would be used
            
            # Add placeholder mappings
            # This is a simplification and would be replaced with actual ZIP-to-county-to-MSA mappings
            # For now, we'll just process the data and note this as an area for improvement
            
            logging.warning("ZIP code to MSA mapping is approximated and needs improvement in production")
            
            # Group by state, tax year, and income bracket for demonstration purposes
            state_year_agg = irs_df.groupby(['STATE', 'tax_year', 'AGI_STUB']).agg({
                'N1': 'sum',  # Number of returns
                'A00100': 'sum',  # AGI
                'A02300': 'sum',  # Unemployment compensation
                'A00200': 'sum',  # Wages and salaries
                'A00900': 'sum',  # Business income
                'A01000': 'sum',  # Capital gains
                'N01000': 'sum',  # Number of returns with capital gains
            }).reset_index()
            
            msa_irs_data['state_year_income'] = state_year_agg
            
            # Create a placeholder MSA-level dataset that would be improved with proper mapping
            # In reality, this would use the ZIP-to-county-to-MSA crosswalk
            placeholder_msa = pd.DataFrame({
                'cbsa_code': ['99999', '88888', '77777'],
                'tax_year': [2022, 2022, 2022],
                'high_income_returns': [10000, 20000, 15000],
                'high_income_agi': [5000000, 10000000, 7500000],
                'high_income_capital_gains': [1000000, 2000000, 1500000]
            })
            placeholder_msa['cbsa_title'] = placeholder_msa['cbsa_code'].map(cbsa_to_title)
            
            msa_irs_data['msa_placeholder'] = placeholder_msa
            
            logging.info("Created placeholder IRS data at MSA level (requires ZIP-to-county mapping)")
        
        # Process individual year files if needed
        year_data = {}
        for k in irs_data.keys():
            if k.isdigit():
                year_data[k] = irs_data[k]
        
        if year_data and not msa_irs_data:
            logging.info("Processing individual year IRS data...")
            # Similar processing as above for individual years
            # This would be implemented with proper ZIP-to-county-to-MSA crosswalk
        
        return msa_irs_data
    
    except Exception as e:
        logging.error(f"Error aggregating IRS data: {e}")
        return {}

def aggregate_fdic_to_msa(fdic_data, crosswalk_data):
    """
    Aggregate FDIC deposit data to MSA level.
    
    Args:
        fdic_data (dict): Dictionary containing FDIC deposit data
        crosswalk_data (dict): Dictionary containing crosswalk data
    
    Returns:
        dict: Dictionary of DataFrames with MSA-level FDIC data
    """
    logging.info("Aggregating FDIC deposit data to MSA level...")
    msa_fdic_data = {}
    
    try:
        # Get years with data
        years = [k for k in fdic_data.keys() if k != 'data_dictionary']
        if not years:
            logging.warning("No FDIC deposit data available for aggregation.")
            return msa_fdic_data
        
        # Extract MSA information from FDIC data
        for year in years:
            df = fdic_data[year].copy()
            
            # Check if the data has MSA information
            if 'MSABR' in df.columns and 'MSANAMB' in df.columns and 'DEPSUMBR' in df.columns:
                logging.info(f"Processing FDIC deposits for {year} with direct MSA mapping...")
                
                # For FDIC data, we can use the MSA codes directly without county crosswalk
                # Group by MSA and sum deposits
                msa_deposits = df.groupby(['MSABR', 'MSANAMB']).agg({
                    'DEPSUMBR': 'sum',
                    'STALPBR': lambda x: sorted(set(x))[0] if len(set(x)) > 0 else None,  # Take the first state
                    'BRNUM': 'count'  # Count branches
                }).reset_index()
                
                # Rename for clarity
                msa_deposits.rename(columns={
                    'MSABR': 'cbsa_code',
                    'MSANAMB': 'cbsa_title',
                    'DEPSUMBR': 'total_deposits',
                    'STALPBR': 'primary_state',
                    'BRNUM': 'branch_count'
                }, inplace=True)
                
                # Add year
                msa_deposits['year'] = year
                
                # Ensure cbsa_code is a string
                msa_deposits['cbsa_code'] = msa_deposits['cbsa_code'].astype(str)
                
                msa_fdic_data[year] = msa_deposits
                logging.info(f"Processed FDIC deposits for {year} with {len(msa_deposits)} MSAs")
        
        # If we have population data, calculate deposits per capita
        # This would use MSA population data calculated earlier
        
        return msa_fdic_data
    
    except Exception as e:
        logging.error(f"Error aggregating FDIC data: {e}")
        return {}

def combine_msa_data(msa_bea, msa_census, msa_irs, msa_fdic, bls_data):
    """
    Combine data from different sources at the MSA level for analysis.
    
    Args:
        msa_bea (dict): Dictionary of BEA MSA-level data
        msa_census (dict): Dictionary of Census MSA-level data
        msa_irs (dict): Dictionary of IRS MSA-level data
        msa_fdic (dict): Dictionary of FDIC MSA-level data
        bls_data (dict): Dictionary of BLS advisor data (already at MSA level)
    
    Returns:
        dict: Dictionary of combined MSA data
    """
    logging.info("Combining MSA-level data from all sources...")
    combined_data = {}
    
    try:
        # Collect all unique MSAs from all datasets
        all_msas = set()
        
        # BEA MSAs
        if 'gdp' in msa_bea:
            all_msas.update(msa_bea['gdp']['cbsa_code'].unique())
        
        # Census MSAs
        if 'acs' in msa_census:
            all_msas.update(msa_census['acs']['cbsa_code'].unique())
        
        # FDIC MSAs (from the most recent year)
        fdic_years = [k for k in msa_fdic.keys() if k != 'data_dictionary']
        if fdic_years:
            most_recent_fdic = sorted(fdic_years)[-1]
            all_msas.update(msa_fdic[most_recent_fdic]['cbsa_code'].unique())
        
        # Create MSA reference table
        msa_info = []
        for cbsa_code in all_msas:
            # Get the name from BEA or Census or FDIC data, in that order of preference
            cbsa_title = None
            if 'gdp' in msa_bea and cbsa_code in msa_bea['gdp']['cbsa_code'].values:
                cbsa_title = msa_bea['gdp'][msa_bea['gdp']['cbsa_code'] == cbsa_code]['cbsa_title'].iloc[0]
            elif 'acs' in msa_census and cbsa_code in msa_census['acs']['cbsa_code'].values:
                cbsa_title = msa_census['acs'][msa_census['acs']['cbsa_code'] == cbsa_code]['cbsa_title'].iloc[0]
            elif fdic_years and cbsa_code in msa_fdic[most_recent_fdic]['cbsa_code'].values:
                cbsa_title = msa_fdic[most_recent_fdic][msa_fdic[most_recent_fdic]['cbsa_code'] == cbsa_code]['cbsa_title'].iloc[0]
            
            if cbsa_title:
                msa_info.append({
                    'cbsa_code': cbsa_code,
                    'cbsa_title': cbsa_title
                })
        
        msa_reference = pd.DataFrame(msa_info)
        logging.info(f"Created MSA reference table with {len(msa_reference)} MSAs")
        combined_data['msa_reference'] = msa_reference
        
        # Combine data for the most recent year available
        # Start with a base year (can be adjusted based on available data)
        recent_year = 2023
        
        # Collect most recent metrics for HNWI Density Metrics
        metrics = pd.DataFrame(msa_reference)
        
        # Add BEA GDP data
        if 'gdp' in msa_bea:
            gdp_recent = msa_bea['gdp'].sort_values('TimePeriod', ascending=False).drop_duplicates('cbsa_code')
            gdp_recent = gdp_recent[['cbsa_code', 'DataValue']]
            gdp_recent.rename(columns={'DataValue': 'gdp_value'}, inplace=True)
            metrics = pd.merge(metrics, gdp_recent, on='cbsa_code', how='left')
        
        # Add BEA GDP growth data
        if 'gdp_growth' in msa_bea:
            # Filter for recent and valid growth rates
            growth_recent = msa_bea['gdp_growth'].sort_values('TimePeriod', ascending=False)
            growth_recent = growth_recent[growth_recent['annual_gdp_growth'].notna()]
            growth_recent = growth_recent.drop_duplicates('cbsa_code')
            
            growth_recent = growth_recent[['cbsa_code', 'annual_gdp_growth']]
            metrics = pd.merge(metrics, growth_recent, on='cbsa_code', how='left')
        
        # Add Census ACS data (luxury homes, high income)
        if 'acs' in msa_census:
            acs_recent = msa_census['acs'].sort_values('year', ascending=False).drop_duplicates('cbsa_code')
            
            acs_metrics = []
            if 'luxury_home_pct' in acs_recent.columns:
                acs_metrics.append('luxury_home_pct')
            if 'high_income_household_pct' in acs_recent.columns:
                acs_metrics.append('high_income_household_pct')
            if 'college_degree_pct' in acs_recent.columns:
                acs_metrics.append('college_degree_pct')
            if 'B01001_001E' in acs_recent.columns:
                acs_metrics.append('B01001_001E')  # Total population
            
            if acs_metrics:
                acs_recent = acs_recent[['cbsa_code'] + acs_metrics]
                # Rename population column if it exists
                if 'B01001_001E' in acs_recent.columns:
                    acs_recent.rename(columns={'B01001_001E': 'total_population'}, inplace=True)
                
                metrics = pd.merge(metrics, acs_recent, on='cbsa_code', how='left')
        
        # Add FDIC deposit data
        if fdic_years:
            most_recent_fdic_data = msa_fdic[most_recent_fdic][['cbsa_code', 'total_deposits', 'branch_count']]
            metrics = pd.merge(metrics, most_recent_fdic_data, on='cbsa_code', how='left')
            
            # Calculate deposit intensity (deposits per capita)
            if 'total_deposits' in metrics.columns and 'total_population' in metrics.columns:
                metrics['deposit_per_capita'] = metrics['total_deposits'] / metrics['total_population']
        
        # Add BLS advisor data
        bls_years = [k for k in bls_data.keys() if k != 'data_dictionary' and k.isdigit()]
        if bls_years:
            most_recent_bls = sorted(bls_years)[-1]
            bls_df = bls_data[most_recent_bls].copy()
            
            # Check if the data has MSA information and advisor counts
            if 'cbsa' in bls_df.columns and 'tot_emp' in bls_df.columns:
                # Group by MSA and sum total employment
                bls_msas = bls_df.groupby('cbsa').agg({
                    'tot_emp': 'sum',
                    'area_title': 'first'  # Keep the first area title
                }).reset_index()
                
                # Rename for consistency
                bls_msas.rename(columns={
                    'cbsa': 'cbsa_code',
                    'tot_emp': 'total_advisors'
                }, inplace=True)
                
                # Keep only relevant columns
                bls_msas = bls_msas[['cbsa_code', 'total_advisors']]
                
                # Merge with metrics
                metrics = pd.merge(metrics, bls_msas, on='cbsa_code', how='left')
                
                # Calculate advisor penetration rate (advisors per 10,000 residents)
                if 'total_advisors' in metrics.columns and 'total_population' in metrics.columns:
                    metrics['advisor_per_10k'] = (metrics['total_advisors'] / metrics['total_population']) * 10000
        
        combined_data['msa_metrics'] = metrics
        logging.info(f"Created combined metrics table with {len(metrics)} MSAs and {len(metrics.columns)} columns")
        
        # Calculate HNWI-to-Advisor ratio if we have both high income data and advisor data
        if all(col in metrics.columns for col in ['high_income_household_pct', 'total_population', 'total_advisors']):
            metrics['high_income_households'] = metrics['high_income_household_pct'] * metrics['total_population'] / 100
            metrics['hnwi_to_advisor_ratio'] = metrics['high_income_households'] / metrics['total_advisors']
            combined_data['msa_metrics'] = metrics
        
        return combined_data
    
    except Exception as e:
        logging.error(f"Error combining MSA data: {e}")
        return {}

def save_output_files(combined_data, msa_bea, msa_census, msa_irs, msa_fdic):
    """
    Save output files for analysis.
    
    Args:
        combined_data (dict): Dictionary of combined MSA data
        msa_bea (dict): Dictionary of BEA MSA-level data
        msa_census (dict): Dictionary of Census MSA-level data
        msa_irs (dict): Dictionary of IRS MSA-level data
        msa_fdic (dict): Dictionary of FDIC MSA-level data
    """
    logging.info("Saving output files...")
    timestamp = datetime.now().strftime("%Y%m%d")
    
    # Save combined data
    if 'msa_metrics' in combined_data:
        metrics_file = os.path.join(OUTPUT_DIR, f"msa_combined_metrics_{timestamp}.csv")
        combined_data['msa_metrics'].to_csv(metrics_file, index=False)
        logging.info(f"Saved combined metrics to {metrics_file}")
    
    if 'msa_reference' in combined_data:
        reference_file = os.path.join(OUTPUT_DIR, f"msa_reference_{timestamp}.csv")
        combined_data['msa_reference'].to_csv(reference_file, index=False)
        logging.info(f"Saved MSA reference to {reference_file}")
    
    # Save BEA MSA data
    for key, df in msa_bea.items():
        bea_file = os.path.join(OUTPUT_DIR, f"msa_bea_{key}_{timestamp}.csv")
        df.to_csv(bea_file, index=False)
        logging.info(f"Saved BEA {key} data to {bea_file}")
    
    # Save Census MSA data
    for key, df in msa_census.items():
        census_file = os.path.join(OUTPUT_DIR, f"msa_census_{key}_{timestamp}.csv")
        df.to_csv(census_file, index=False)
        logging.info(f"Saved Census {key} data to {census_file}")
    
    # Save FDIC MSA data
    for key, df in msa_fdic.items():
        if key != 'data_dictionary':
            fdic_file = os.path.join(OUTPUT_DIR, f"msa_fdic_{key}_{timestamp}.csv")
            df.to_csv(fdic_file, index=False)
            logging.info(f"Saved FDIC {key} data to {fdic_file}")
    
    # Create a readme file for the output directory
    readme_content = f"""# MSA-Level Wealth Management Market Opportunity Data

This directory contains Metropolitan Statistical Area (MSA) level data aggregated from various sources for the Wealth Management Market Opportunity analysis.

## Files Generated on {timestamp}

### Combined Data
- `msa_combined_metrics_{timestamp}.csv`: Combined metrics for wealth management opportunity analysis
- `msa_reference_{timestamp}.csv`: Reference table of all MSAs

### BEA Data (Bureau of Economic Analysis)
- `msa_bea_gdp_{timestamp}.csv`: GDP data by MSA
- `msa_bea_gdp_growth_{timestamp}.csv`: GDP growth rates by MSA
- `msa_bea_population_{timestamp}.csv`: Population data by MSA
- `msa_bea_personal_income_{timestamp}.csv`: Personal income data by MSA
- `msa_bea_per_capita_income_calculated_{timestamp}.csv`: Calculated per capita income by MSA
- `msa_bea_income_by_industry_{timestamp}.csv`: Detailed income by industry data at MSA level

### Census ACS Data (American Community Survey)
- `msa_census_acs_{timestamp}.csv`: Demographics and housing data at MSA level

### FDIC Data (Federal Deposit Insurance Corporation)
- `msa_fdic_YYYY_{timestamp}.csv`: Deposit data by MSA for each year YYYY

## Key Metrics in Combined File

1. **HNWI Density Metrics**
   - Luxury Real Estate Quotient: `luxury_home_pct` (% of homes valued over $1M)
   - Income Elite Ratio: `high_income_household_pct` (% of households with income $200K+)
   - Banking Deposit Intensity: `deposit_per_capita` (FDIC deposits per capita)

2. **Financial Services Coverage Metrics**
   - Advisor Penetration Rate: `advisor_per_10k` (Registered advisors per 10,000 residents)
   - HNWI-to-Advisor Ratio: `hnwi_to_advisor_ratio` (Estimated high-income households per advisor)

3. **Economic Vitality Indicators**
   - GDP Growth Trend: `annual_gdp_growth` (Annual GDP growth rate)
   - Wealth-Creating Industries: Detailed in industry-specific files

## Notes

- MSAs are identified by their CBSA (Core Based Statistical Area) code
- Data is aggregated from county to MSA level using Census Bureau CBSA definitions
- Time periods vary by data source, with an effort to use the most recent data available
- Missing values may indicate that the MSA was not covered in the source data
"""
    
    readme_file = os.path.join(OUTPUT_DIR, "README.md")
    with open(readme_file, 'w') as f:
        f.write(readme_content)
    logging.info(f"Created README file at {readme_file}")
    
    logging.info("All output files saved successfully")

def main():
    """Main function to aggregate county data to MSA level and calculate metrics"""
    logging.info("Starting county-to-MSA aggregation process...")
    
    # Load crosswalk data
    crosswalk_data = load_crosswalk()
    if crosswalk_data is None:
        logging.error("Failed to load crosswalk data. Exiting.")
        return
    
    # Load data from various sources
    bea_data = load_bea_data()
    census_data = load_census_acs_data()
    irs_data = load_irs_soi_data()
    fdic_data = load_fdic_deposit_data()
    bls_data = load_bls_advisor_data()
    
    # Aggregate data to MSA level
    msa_bea = aggregate_bea_to_msa(bea_data, crosswalk_data)
    msa_census = aggregate_census_to_msa(census_data, crosswalk_data)
    msa_irs = aggregate_irs_to_msa(irs_data, crosswalk_data)
    msa_fdic = aggregate_fdic_to_msa(fdic_data, crosswalk_data)
    
    # Combine data for analysis
    combined_data = combine_msa_data(msa_bea, msa_census, msa_irs, msa_fdic, bls_data)
    
    # Save output files
    save_output_files(combined_data, msa_bea, msa_census, msa_irs, msa_fdic)
    
    logging.info("County-to-MSA aggregation process complete!")

if __name__ == "__main__":
    main()