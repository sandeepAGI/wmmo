#!/usr/bin/env python3
"""
hnwi_density_metrics.py

This script calculates High-Net-Worth Individual (HNWI) density metrics for each
Metropolitan Statistical Area (MSA) using the aggregated data. These metrics include:

1. HNWI Density Index: Composite metric of high-income households
2. Wealth Growth Rate: Year-over-year changes in wealth indicators
3. Luxury Real Estate Quotient: Concentration of high-value homes
4. Income Elite Ratio: Concentration of high-income households
5. Banking Deposit Intensity: Total deposits per capita

These metrics are designed to identify areas with high concentrations of wealth.
"""

import os
import sys
import pandas as pd
import numpy as np
import glob
import logging
from pathlib import Path
from datetime import datetime

# Add parent directory to path to import modules from other directories
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("hnwi_density_metrics.log"),
        logging.StreamHandler()
    ]
)

# Constants
MSA_DATA_DIR = os.path.join(parent_dir, "msadata")
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

def load_msa_data():
    """
    Load MSA-level data files.
    
    Returns:
        dict: Dictionary of DataFrames with MSA data
    """
    logging.info("Loading MSA-level data...")
    msa_data = {}
    
    try:
        # Find the most recent combined metrics file
        combined_files = glob.glob(os.path.join(MSA_DATA_DIR, "msa_combined_metrics_*.csv"))
        if combined_files:
            most_recent = sorted(combined_files)[-1]
            logging.info(f"Loading combined metrics from {most_recent}")
            msa_data['combined'] = pd.read_csv(most_recent)
        
        # Load BEA GDP data (for trend analysis)
        gdp_files = glob.glob(os.path.join(MSA_DATA_DIR, "msa_bea_gdp_*.csv"))
        if gdp_files:
            most_recent = sorted(gdp_files)[-1]
            logging.info(f"Loading BEA GDP data from {most_recent}")
            msa_data['gdp'] = pd.read_csv(most_recent)
        
        # Load Census ACS data
        acs_files = glob.glob(os.path.join(MSA_DATA_DIR, "msa_census_acs_*.csv"))
        if acs_files:
            most_recent = sorted(acs_files)[-1]
            logging.info(f"Loading Census ACS data from {most_recent}")
            msa_data['acs'] = pd.read_csv(most_recent)
        
        # Load FDIC data (for multiple years)
        fdic_files = glob.glob(os.path.join(MSA_DATA_DIR, "msa_fdic_*.csv"))
        if fdic_files:
            # Extract year from filename
            fdic_years = {}
            for file_path in fdic_files:
                try:
                    # Extract year from filename pattern msa_fdic_YYYY_YYYYMMDD.csv
                    file_name = os.path.basename(file_path)
                    parts = file_name.split('_')
                    if len(parts) >= 3 and parts[2].isdigit() and len(parts[2]) == 4:
                        year = parts[2]
                        fdic_years[year] = file_path
                except:
                    continue
            
            # Load each year's data
            for year, file_path in fdic_years.items():
                logging.info(f"Loading FDIC {year} data from {file_path}")
                msa_data[f'fdic_{year}'] = pd.read_csv(file_path)
        
        logging.info(f"Loaded MSA data: {', '.join(msa_data.keys())}")
        return msa_data
    
    except Exception as e:
        logging.error(f"Error loading MSA data: {e}")
        return {}

def calculate_hnwi_density_index(combined_df):
    """
    Calculate a composite HNWI Density Index from several wealth indicators.
    
    Args:
        combined_df (pandas.DataFrame): Combined MSA metrics
    
    Returns:
        pandas.DataFrame: DataFrame with HNWI Density Index
    """
    logging.info("Calculating HNWI Density Index...")
    try:
        # Make a copy of the input data
        df = combined_df.copy()
        
        # Define the columns to use for the index (if available)
        wealth_indicators = []
        if 'high_income_household_pct' in df.columns:
            wealth_indicators.append('high_income_household_pct')
        if 'luxury_home_pct' in df.columns:
            wealth_indicators.append('luxury_home_pct')
        if 'deposit_per_capita' in df.columns:
            wealth_indicators.append('deposit_per_capita')
        if 'college_degree_pct' in df.columns:
            wealth_indicators.append('college_degree_pct')
        
        if not wealth_indicators:
            logging.warning("No wealth indicators available to calculate HNWI Density Index")
            return df
        
        # Normalize each indicator to a 0-100 scale
        for indicator in wealth_indicators:
            min_val = df[indicator].min()
            max_val = df[indicator].max()
            if max_val > min_val:
                df[f'{indicator}_norm'] = 100 * (df[indicator] - min_val) / (max_val - min_val)
            else:
                df[f'{indicator}_norm'] = 0
        
        # Calculate the HNWI Density Index as the average of normalized indicators
        norm_indicators = [f'{indicator}_norm' for indicator in wealth_indicators]
        df['hnwi_density_index'] = df[norm_indicators].mean(axis=1)
        
        logging.info(f"Calculated HNWI Density Index using {len(wealth_indicators)} indicators")
        return df
    
    except Exception as e:
        logging.error(f"Error calculating HNWI Density Index: {e}")
        return combined_df

def calculate_wealth_growth_rate(msa_data):
    """
    Calculate wealth growth rates using year-over-year changes in key wealth indicators.
    
    Args:
        msa_data (dict): Dictionary of MSA data DataFrames
    
    Returns:
        pandas.DataFrame: DataFrame with wealth growth metrics
    """
    logging.info("Calculating Wealth Growth Rate...")
    try:
        # Check if we have GDP data for multiple years
        if 'gdp' in msa_data:
            gdp_df = msa_data['gdp'].copy()
            
            # Make sure we have cbsa_code, TimePeriod, and DataValue columns
            if all(col in gdp_df.columns for col in ['cbsa_code', 'TimePeriod', 'DataValue']):
                # Convert columns to appropriate types
                gdp_df['TimePeriod'] = pd.to_numeric(gdp_df['TimePeriod'], errors='coerce')
                gdp_df['DataValue'] = pd.to_numeric(gdp_df['DataValue'], errors='coerce')
                
                # Filter for valid data
                gdp_df = gdp_df.dropna(subset=['cbsa_code', 'TimePeriod', 'DataValue'])
                
                # Calculate 5-year CAGR for each MSA
                gdp_growth = []
                
                for cbsa_code in gdp_df['cbsa_code'].unique():
                    msa_gdp = gdp_df[gdp_df['cbsa_code'] == cbsa_code].sort_values('TimePeriod')
                    
                    if len(msa_gdp) >= 2:
                        # Get earliest and latest years (within last 6 years)
                        latest_year = msa_gdp['TimePeriod'].max()
                        earliest_year = msa_gdp['TimePeriod'].min()
                        
                        # Aim for 5-year CAGR, but use what's available
                        if latest_year - earliest_year >= 3:  # At least 3 years of data
                            start_gdp = msa_gdp[msa_gdp['TimePeriod'] == earliest_year]['DataValue'].iloc[0]
                            end_gdp = msa_gdp[msa_gdp['TimePeriod'] == latest_year]['DataValue'].iloc[0]
                            years = latest_year - earliest_year
                            
                            # Calculate CAGR
                            cagr = (end_gdp / start_gdp) ** (1 / years) - 1
                            
                            gdp_growth.append({
                                'cbsa_code': cbsa_code,
                                'start_year': earliest_year,
                                'end_year': latest_year,
                                'years_measured': years,
                                'start_gdp': start_gdp,
                                'end_gdp': end_gdp,
                                'gdp_cagr': cagr
                            })
                
                gdp_growth_df = pd.DataFrame(gdp_growth)
                
                if 'combined' in msa_data:
                    # Merge with combined metrics
                    result = pd.merge(
                        msa_data['combined'],
                        gdp_growth_df[['cbsa_code', 'gdp_cagr', 'years_measured']],
                        on='cbsa_code',
                        how='left'
                    )
                    
                    logging.info(f"Calculated GDP CAGR for {len(gdp_growth_df)} MSAs")
                    return result
                else:
                    return gdp_growth_df
            else:
                logging.warning("GDP data doesn't have required columns for growth calculation")
        
        logging.warning("No suitable data available to calculate wealth growth rate")
        return msa_data.get('combined', pd.DataFrame())
    
    except Exception as e:
        logging.error(f"Error calculating Wealth Growth Rate: {e}")
        return msa_data.get('combined', pd.DataFrame())

def calculate_luxury_real_estate_quotient(msa_data):
    """
    Calculate Luxury Real Estate Quotient using high-value homes concentration.
    
    Args:
        msa_data (dict): Dictionary of MSA data DataFrames
    
    Returns:
        pandas.DataFrame: DataFrame with luxury real estate metrics
    """
    logging.info("Calculating Luxury Real Estate Quotient...")
    try:
        # Check if we already have luxury_home_pct in combined data
        if 'combined' in msa_data and 'luxury_home_pct' in msa_data['combined'].columns:
            logging.info("Using pre-calculated Luxury Real Estate Quotient")
            return msa_data['combined']
        
        # If not available in combined data, check if we have ACS data
        if 'acs' in msa_data:
            acs_df = msa_data['acs'].copy()
            
            # Check if we have the necessary columns
            luxury_home_cols = [col for col in ['B25075_022E', 'B25075_023E', 'B25075_024E', 'B25075_001E'] 
                               if col in acs_df.columns]
            
            if len(luxury_home_cols) >= 4:  # Need all 4 columns
                # Calculate luxury home percentage
                acs_df['luxury_home_count'] = acs_df['B25075_022E'] + acs_df['B25075_023E'] + acs_df['B25075_024E']
                acs_df['luxury_home_pct'] = (acs_df['luxury_home_count'] / acs_df['B25075_001E']) * 100
                
                # Keep only relevant columns
                luxury_df = acs_df[['cbsa_code', 'cbsa_title', 'luxury_home_count', 'luxury_home_pct']].copy()
                
                if 'combined' in msa_data:
                    # Merge with combined metrics
                    result = pd.merge(
                        msa_data['combined'],
                        luxury_df[['cbsa_code', 'luxury_home_count', 'luxury_home_pct']],
                        on='cbsa_code',
                        how='left'
                    )
                    
                    logging.info(f"Calculated Luxury Real Estate Quotient for {len(luxury_df)} MSAs")
                    return result
                else:
                    return luxury_df
            else:
                logging.warning("ACS data doesn't have necessary columns for luxury home calculation")
        
        logging.warning("No suitable data available to calculate Luxury Real Estate Quotient")
        return msa_data.get('combined', pd.DataFrame())
    
    except Exception as e:
        logging.error(f"Error calculating Luxury Real Estate Quotient: {e}")
        return msa_data.get('combined', pd.DataFrame())

def calculate_income_elite_ratio(msa_data):
    """
    Calculate Income Elite Ratio using high-income household concentration.
    
    Args:
        msa_data (dict): Dictionary of MSA data DataFrames
    
    Returns:
        pandas.DataFrame: DataFrame with income elite metrics
    """
    logging.info("Calculating Income Elite Ratio...")
    try:
        # Check if we already have high_income_household_pct in combined data
        if 'combined' in msa_data and 'high_income_household_pct' in msa_data['combined'].columns:
            logging.info("Using pre-calculated Income Elite Ratio")
            return msa_data['combined']
        
        # If not available in combined data, check if we have ACS data
        if 'acs' in msa_data:
            acs_df = msa_data['acs'].copy()
            
            # Check if we have the necessary columns
            if all(col in acs_df.columns for col in ['B19001_017E', 'B19001_001E']):
                # Calculate high-income household percentage
                acs_df['high_income_household_pct'] = (acs_df['B19001_017E'] / acs_df['B19001_001E']) * 100
                
                # Keep only relevant columns
                income_df = acs_df[['cbsa_code', 'cbsa_title', 'B19001_017E', 'B19001_001E', 'high_income_household_pct']].copy()
                income_df.rename(columns={
                    'B19001_017E': 'high_income_households',
                    'B19001_001E': 'total_households'
                }, inplace=True)
                
                if 'combined' in msa_data:
                    # Merge with combined metrics
                    result = pd.merge(
                        msa_data['combined'],
                        income_df[['cbsa_code', 'high_income_households', 'total_households', 'high_income_household_pct']],
                        on='cbsa_code',
                        how='left'
                    )
                    
                    logging.info(f"Calculated Income Elite Ratio for {len(income_df)} MSAs")
                    return result
                else:
                    return income_df
            else:
                logging.warning("ACS data doesn't have necessary columns for high-income household calculation")
        
        logging.warning("No suitable data available to calculate Income Elite Ratio")
        return msa_data.get('combined', pd.DataFrame())
    
    except Exception as e:
        logging.error(f"Error calculating Income Elite Ratio: {e}")
        return msa_data.get('combined', pd.DataFrame())

def calculate_banking_deposit_intensity(msa_data):
    """
    Calculate Banking Deposit Intensity using deposits per capita.
    
    Args:
        msa_data (dict): Dictionary of MSA data DataFrames
    
    Returns:
        pandas.DataFrame: DataFrame with banking deposit metrics
    """
    logging.info("Calculating Banking Deposit Intensity...")
    try:
        # Check if we already have deposit_per_capita in combined data
        if 'combined' in msa_data and 'deposit_per_capita' in msa_data['combined'].columns:
            logging.info("Using pre-calculated Banking Deposit Intensity")
            return msa_data['combined']
        
        # If not available in combined data, look for FDIC data from the most recent year
        fdic_keys = [k for k in msa_data.keys() if k.startswith('fdic_')]
        if fdic_keys:
            most_recent_key = sorted(fdic_keys)[-1]
            fdic_df = msa_data[most_recent_key].copy()
            
            # Check if we have the necessary columns
            if 'total_deposits' in fdic_df.columns:
                deposits_df = fdic_df[['cbsa_code', 'cbsa_title', 'total_deposits', 'branch_count']].copy()
                
                # If we have population data in combined metrics, calculate per capita
                if 'combined' in msa_data and 'total_population' in msa_data['combined'].columns:
                    combined_df = msa_data['combined'].copy()
                    
                    # Merge deposits with population
                    result = pd.merge(
                        combined_df,
                        deposits_df,
                        on='cbsa_code',
                        how='left',
                        suffixes=('', '_fdic')
                    )
                    
                    # Calculate deposits per capita
                    mask = (result['total_population'].notna()) & (result['total_population'] > 0)
                    result.loc[mask, 'deposit_per_capita'] = result.loc[mask, 'total_deposits'] / result.loc[mask, 'total_population']
                    
                    # Calculate branches per 100k population
                    result.loc[mask, 'branches_per_100k'] = result.loc[mask, 'branch_count'] / result.loc[mask, 'total_population'] * 100000
                    
                    logging.info(f"Calculated Banking Deposit Intensity for {mask.sum()} MSAs")
                    return result
                else:
                    logging.warning("No population data available to calculate deposits per capita")
                    return msa_data.get('combined', pd.DataFrame())
            else:
                logging.warning("FDIC data doesn't have necessary columns for deposit calculation")
        
        logging.warning("No suitable data available to calculate Banking Deposit Intensity")
        return msa_data.get('combined', pd.DataFrame())
    
    except Exception as e:
        logging.error(f"Error calculating Banking Deposit Intensity: {e}")
        return msa_data.get('combined', pd.DataFrame())

def create_hnwi_density_ranking(metrics_df):
    """
    Create a ranking of MSAs based on HNWI density metrics.
    
    Args:
        metrics_df (pandas.DataFrame): DataFrame with all calculated metrics
    
    Returns:
        pandas.DataFrame: DataFrame with rankings
    """
    logging.info("Creating HNWI density rankings...")
    try:
        df = metrics_df.copy()
        
        # Define the metrics to use for ranking
        ranking_metrics = []
        if 'hnwi_density_index' in df.columns:
            ranking_metrics.append('hnwi_density_index')
        if 'luxury_home_pct' in df.columns:
            ranking_metrics.append('luxury_home_pct')
        if 'high_income_household_pct' in df.columns:
            ranking_metrics.append('high_income_household_pct')
        if 'deposit_per_capita' in df.columns:
            ranking_metrics.append('deposit_per_capita')
        if 'gdp_cagr' in df.columns:
            ranking_metrics.append('gdp_cagr')
        
        if not ranking_metrics:
            logging.warning("No metrics available for ranking")
            return df
        
        # Create individual rankings for each metric
        for metric in ranking_metrics:
            df[f'{metric}_rank'] = df[metric].rank(ascending=False, method='min')
        
        # Create a composite rank (average of individual ranks)
        rank_columns = [f'{metric}_rank' for metric in ranking_metrics]
        df['composite_hnwi_rank'] = df[rank_columns].mean(axis=1)
        
        # Sort by composite rank
        df = df.sort_values('composite_hnwi_rank')
        
        # Add a simple rank column
        df['hnwi_density_rank'] = range(1, len(df) + 1)
        
        logging.info(f"Created HNWI density rankings using {len(ranking_metrics)} metrics")
        return df
    
    except Exception as e:
        logging.error(f"Error creating HNWI density rankings: {e}")
        return metrics_df

def save_results(metrics_df):
    """
    Save the results to CSV files.
    
    Args:
        metrics_df (pandas.DataFrame): DataFrame with all calculated metrics
    """
    logging.info("Saving results...")
    timestamp = datetime.now().strftime("%Y%m%d")
    
    try:
        # Save complete metrics file
        metrics_file = os.path.join(OUTPUT_DIR, f"hnwi_density_metrics_{timestamp}.csv")
        metrics_df.to_csv(metrics_file, index=False)
        logging.info(f"Saved complete metrics to {metrics_file}")
        
        # Save a ranked list with key metrics
        key_metrics = ['cbsa_code', 'cbsa_title']
        
        # Add available metrics columns
        for col in ['hnwi_density_index', 'high_income_household_pct', 'luxury_home_pct', 
                   'deposit_per_capita', 'gdp_cagr', 'hnwi_density_rank']:
            if col in metrics_df.columns:
                key_metrics.append(col)
        
        # Filter and sort by rank
        if 'hnwi_density_rank' in metrics_df.columns:
            ranked_df = metrics_df[key_metrics].sort_values('hnwi_density_rank')
            ranked_file = os.path.join(OUTPUT_DIR, f"hnwi_density_rankings_{timestamp}.csv")
            ranked_df.to_csv(ranked_file, index=False)
            logging.info(f"Saved rankings to {ranked_file}")
        
        # Create a documentation file
        doc_content = f"""# HNWI Density Metrics

This file contains High-Net-Worth Individual (HNWI) density metrics for Metropolitan Statistical Areas (MSAs).

## Metrics Calculated

1. **HNWI Density Index**: A composite index of various wealth indicators, normalized to a 0-100 scale.
   - Higher values indicate greater concentrations of wealth.
   - Calculated using: high-income households, luxury homes, deposits per capita, college education

2. **Wealth Growth Rate**: The compound annual growth rate (CAGR) of MSA GDP over the available time period.
   - Shows how quickly wealth is being created in the MSA.

3. **Luxury Real Estate Quotient**: The percentage of owner-occupied homes valued at $1 million or more.
   - Indicator of high-end real estate markets.

4. **Income Elite Ratio**: The percentage of households with income of $200,000 or more.
   - Direct measure of high-income concentration.

5. **Banking Deposit Intensity**: Total FDIC-insured deposits per capita.
   - Indicator of banking wealth in the region.

## Rankings

The MSAs are ranked based on a composite of the above metrics. The composite ranking is the average of the individual metric rankings, with lower numbers indicating better ranks.

## Files Generated on {timestamp}

- `hnwi_density_metrics_{timestamp}.csv`: Complete metrics for all MSAs
- `hnwi_density_rankings_{timestamp}.csv`: Simplified rankings with key metrics

## Notes

- Data is aggregated from various sources including BEA, Census ACS, and FDIC
- Missing values indicate the data was not available for that MSA
- Rankings should be interpreted with caution when data is incomplete
"""
        
        doc_file = os.path.join(OUTPUT_DIR, "hnwi_density_metrics_documentation.md")
        with open(doc_file, 'w') as f:
            f.write(doc_content)
        logging.info(f"Created documentation file at {doc_file}")
        
    except Exception as e:
        logging.error(f"Error saving results: {e}")

def main():
    """Main function to calculate HNWI density metrics"""
    logging.info("Starting HNWI density metrics calculation...")
    
    # Load MSA data
    msa_data = load_msa_data()
    if not msa_data or 'combined' not in msa_data:
        logging.error("Failed to load required MSA data. Exiting.")
        return
    
    # Start with the combined metrics
    metrics_df = msa_data['combined'].copy()
    
    # Calculate each metric
    metrics_df = calculate_hnwi_density_index(metrics_df)
    metrics_df = calculate_wealth_growth_rate(msa_data)
    metrics_df = calculate_luxury_real_estate_quotient(msa_data)
    metrics_df = calculate_income_elite_ratio(msa_data)
    metrics_df = calculate_banking_deposit_intensity(msa_data)
    
    # Create rankings
    metrics_df = create_hnwi_density_ranking(metrics_df)
    
    # Save results
    save_results(metrics_df)
    
    logging.info("HNWI density metrics calculation complete!")

if __name__ == "__main__":
    main()