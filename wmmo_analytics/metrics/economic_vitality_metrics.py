#!/usr/bin/env python3
"""
economic_vitality_metrics.py

This script calculates Economic Vitality indicators for each
Metropolitan Statistical Area (MSA) using the aggregated data. These metrics include:

1. GDP Growth Trend: 5-year CAGR of MSA GDP
2. Wealth-Creating Industries: Distribution and growth of wealth-generating industries
3. Business Formation Rate: Rate of new business establishments (requires additional data)
4. Executive Density: Concentration of executives (partial data from BLS)
5. Entrepreneur Quotient: Business exits and liquidity events (requires additional data)

These metrics help identify markets with strong economic fundamentals for wealth management.
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
        logging.FileHandler("economic_vitality_metrics.log"),
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
        
        # Load BEA GDP data for trends
        gdp_files = glob.glob(os.path.join(MSA_DATA_DIR, "msa_bea_gdp_*.csv"))
        if gdp_files:
            most_recent = sorted(gdp_files)[-1]
            logging.info(f"Loading BEA GDP data from {most_recent}")
            msa_data['gdp'] = pd.read_csv(most_recent)
        
        # Load industry-specific data if available
        industry_files = glob.glob(os.path.join(MSA_DATA_DIR, "msa_bea_income_by_industry_*.csv"))
        if industry_files:
            most_recent = sorted(industry_files)[-1]
            logging.info(f"Loading industry data from {most_recent}")
            msa_data['industry'] = pd.read_csv(most_recent)
        
        # Load industry pivot table if available
        industry_pivot_files = glob.glob(os.path.join(MSA_DATA_DIR, "msa_bea_income_by_industry_pivot_*.csv"))
        if industry_pivot_files:
            most_recent = sorted(industry_pivot_files)[-1]
            logging.info(f"Loading industry pivot data from {most_recent}")
            msa_data['industry_pivot'] = pd.read_csv(most_recent)
        
        logging.info(f"Loaded MSA data: {', '.join(msa_data.keys())}")
        return msa_data
    
    except Exception as e:
        logging.error(f"Error loading MSA data: {e}")
        return {}

def load_other_metrics():
    """
    Load previously calculated metrics if available.
    
    Returns:
        dict: Dictionary of DataFrames with previously calculated metrics
    """
    logging.info("Checking for previously calculated metrics...")
    metrics = {}
    
    try:
        # Look for HNWI metrics file
        hnwi_files = glob.glob(os.path.join(OUTPUT_DIR, "hnwi_density_metrics_*.csv"))
        if hnwi_files:
            most_recent = sorted(hnwi_files)[-1]
            logging.info(f"Loading HNWI metrics from {most_recent}")
            metrics['hnwi'] = pd.read_csv(most_recent)
        
        # Look for financial services metrics file
        fs_files = glob.glob(os.path.join(OUTPUT_DIR, "financial_services_metrics_*.csv"))
        if fs_files:
            most_recent = sorted(fs_files)[-1]
            logging.info(f"Loading financial services metrics from {most_recent}")
            metrics['fs'] = pd.read_csv(most_recent)
        
        if metrics:
            logging.info(f"Loaded previously calculated metrics: {', '.join(metrics.keys())}")
        else:
            logging.warning("No previously calculated metrics found")
        
        return metrics
    
    except Exception as e:
        logging.error(f"Error loading previously calculated metrics: {e}")
        return {}

def calculate_gdp_growth_trend(msa_data):
    """
    Calculate GDP Growth Trend (5-year CAGR).
    
    Args:
        msa_data (dict): Dictionary of MSA data DataFrames
    
    Returns:
        pandas.DataFrame: DataFrame with GDP growth metrics
    """
    logging.info("Calculating GDP Growth Trend...")
    try:
        # Check if we already have gdp_cagr in combined data
        if 'combined' in msa_data and 'gdp_cagr' in msa_data['combined'].columns:
            logging.info("Using pre-calculated GDP Growth Trend")
            return msa_data['combined']
        
        # If not available in combined data, check if we have GDP data
        if 'gdp' not in msa_data:
            logging.warning("No GDP data available for growth calculation")
            return msa_data.get('combined', pd.DataFrame())
        
        gdp_df = msa_data['gdp'].copy()
        
        # Check if we have the necessary columns
        if all(col in gdp_df.columns for col in ['cbsa_code', 'TimePeriod', 'DataValue']):
            # Ensure data types are correct
            gdp_df['TimePeriod'] = pd.to_numeric(gdp_df['TimePeriod'], errors='coerce')
            gdp_df['DataValue'] = pd.to_numeric(gdp_df['DataValue'], errors='coerce')
            
            # Calculate GDP growth trend for each MSA
            growth_metrics = []
            
            for cbsa_code in gdp_df['cbsa_code'].unique():
                msa_gdp = gdp_df[gdp_df['cbsa_code'] == cbsa_code].sort_values('TimePeriod')
                
                if len(msa_gdp) >= 2:
                    # Get earliest and latest years (within last 6 years)
                    latest_year = msa_gdp['TimePeriod'].max()
                    years_available = msa_gdp['TimePeriod'].unique()
                    
                    # Find earliest year that's at most 5 years before latest
                    target_years = min(5, len(years_available) - 1)  # Aim for 5 years or what's available
                    if target_years > 0:
                        # Get the first and last data points
                        start_year = latest_year - target_years
                        start_gdp = msa_gdp[msa_gdp['TimePeriod'] >= start_year].iloc[0]['DataValue']
                        end_gdp = msa_gdp[msa_gdp['TimePeriod'] == latest_year]['DataValue'].iloc[0]
                        
                        # Calculate CAGR
                        years = latest_year - msa_gdp[msa_gdp['TimePeriod'] >= start_year].iloc[0]['TimePeriod']
                        if years > 0 and start_gdp > 0:
                            cagr = (end_gdp / start_gdp) ** (1 / years) - 1
                            
                            # Get the MSA name
                            cbsa_title = None
                            if 'cbsa_title' in msa_gdp.columns:
                                cbsa_title = msa_gdp['cbsa_title'].iloc[0]
                            
                            growth_metrics.append({
                                'cbsa_code': cbsa_code,
                                'cbsa_title': cbsa_title,
                                'start_year': int(msa_gdp[msa_gdp['TimePeriod'] >= start_year].iloc[0]['TimePeriod']),
                                'end_year': int(latest_year),
                                'years_measured': years,
                                'start_gdp': start_gdp,
                                'end_gdp': end_gdp,
                                'gdp_cagr': cagr,
                                'gdp_total_growth': (end_gdp / start_gdp) - 1
                            })
            
            growth_df = pd.DataFrame(growth_metrics)
            
            if 'combined' in msa_data:
                # Merge with combined metrics
                result = pd.merge(
                    msa_data['combined'],
                    growth_df[['cbsa_code', 'gdp_cagr', 'years_measured', 'gdp_total_growth']],
                    on='cbsa_code',
                    how='left'
                )
                
                logging.info(f"Calculated GDP Growth Trend for {len(growth_df)} MSAs")
                return result
            else:
                return growth_df
        else:
            missing = [col for col in ['cbsa_code', 'TimePeriod', 'DataValue'] if col not in gdp_df.columns]
            logging.warning(f"GDP data doesn't have necessary columns: {missing}")
            return msa_data.get('combined', pd.DataFrame())
    
    except Exception as e:
        logging.error(f"Error calculating GDP Growth Trend: {e}")
        return msa_data.get('combined', pd.DataFrame())

def analyze_wealth_creating_industries(msa_data):
    """
    Analyze wealth-creating industries in each MSA.
    
    Args:
        msa_data (dict): Dictionary of MSA data DataFrames
    
    Returns:
        pandas.DataFrame: DataFrame with industry analysis metrics
    """
    logging.info("Analyzing wealth-creating industries...")
    try:
        # Check if we have industry data
        if 'industry' not in msa_data:
            logging.warning("No industry data available for analysis")
            return msa_data.get('combined', pd.DataFrame())
        
        industry_df = msa_data['industry'].copy()
        
        # Check if we have the necessary columns
        if all(col in industry_df.columns for col in ['cbsa_code', 'TimePeriod', 'LineCode', 'Description', 'DataValue']):
            # Focus on the most recent year
            latest_year = industry_df['TimePeriod'].max()
            latest_data = industry_df[industry_df['TimePeriod'] == latest_year]
            
            # Define key wealth-creating industries by line codes
            # These line codes are specific to BEA CAINC5N table
            wealth_industries = {
                '0700': 'Finance and insurance',  # Financial sector
                '0800': 'Real estate and rental and leasing',  # Real estate
                '0400': 'Manufacturing',  # Manufacturing
                '0900': 'Professional, scientific, and technical services',  # Professional services
                '1102': 'Management of companies and enterprises',  # Corporate management
                '2001': 'Wages and salaries',  # Overall wages
                '2012': 'Proprietors' income',  # Business owners
            }
            
            # Filter for wealth-creating industries
            wealth_data = latest_data[latest_data['LineCode'].isin(wealth_industries.keys())]
            
            # Calculate industry concentration for each MSA
            industry_metrics = []
            
            for cbsa_code in wealth_data['cbsa_code'].unique():
                msa_industries = wealth_data[wealth_data['cbsa_code'] == cbsa_code]
                
                # Get MSA name
                cbsa_title = None
                if 'cbsa_title' in msa_industries.columns:
                    cbsa_title = msa_industries['cbsa_title'].iloc[0]
                
                # Calculate total income for scaling
                total_income = msa_industries['DataValue'].sum()
                
                if total_income > 0:
                    # Get each industry's share
                    industry_shares = {}
                    for _, row in msa_industries.iterrows():
                        industry_code = row['LineCode']
                        industry_name = wealth_industries.get(industry_code, row['Description'])
                        industry_value = row['DataValue']
                        industry_share = industry_value / total_income
                        
                        industry_shares[f'share_{industry_code}'] = industry_share
                    
                    # Create metrics record
                    metrics = {
                        'cbsa_code': cbsa_code,
                        'cbsa_title': cbsa_title,
                        'total_wealth_industries': total_income
                    }
                    metrics.update(industry_shares)
                    
                    # Calculate wealth industry concentration index
                    # Higher values indicate more concentrated in wealth-creating industries
                    metrics['wealth_industry_concentration'] = sum(industry_shares.values())
                    
                    industry_metrics.append(metrics)
            
            industry_analysis = pd.DataFrame(industry_metrics)
            
            if 'combined' in msa_data:
                # Merge with combined metrics
                # Just keep the concentration index to avoid too many columns
                result = pd.merge(
                    msa_data['combined'],
                    industry_analysis[['cbsa_code', 'wealth_industry_concentration']],
                    on='cbsa_code',
                    how='left'
                )
                
                logging.info(f"Analyzed wealth-creating industries for {len(industry_analysis)} MSAs")
                
                # Save detailed industry analysis separately
                timestamp = datetime.now().strftime("%Y%m%d")
                detail_file = os.path.join(OUTPUT_DIR, f"industry_analysis_detail_{timestamp}.csv")
                industry_analysis.to_csv(detail_file, index=False)
                logging.info(f"Saved detailed industry analysis to {detail_file}")
                
                return result
            else:
                return industry_analysis
        else:
            missing = [col for col in ['cbsa_code', 'TimePeriod', 'LineCode', 'Description', 'DataValue'] 
                      if col not in industry_df.columns]
            logging.warning(f"Industry data doesn't have necessary columns: {missing}")
            return msa_data.get('combined', pd.DataFrame())
    
    except Exception as e:
        logging.error(f"Error analyzing wealth-creating industries: {e}")
        return msa_data.get('combined', pd.DataFrame())

def estimate_executive_density(msa_data):
    """
    Estimate executive density in each MSA using partial BLS data.
    
    Args:
        msa_data (dict): Dictionary of MSA data DataFrames
    
    Returns:
        pandas.DataFrame: DataFrame with executive density metrics
    """
    logging.info("Estimating executive density...")
    try:
        # This is a placeholder for executive density calculation
        # In a real implementation, we would use BLS occupational data
        # to identify executive concentration in each MSA
        
        # For this implementation, we'll use income as a proxy
        if 'combined' in msa_data and 'high_income_household_pct' in msa_data['combined'].columns:
            df = msa_data['combined'].copy()
            
            # Use high income as a proxy for executive density
            # This is not perfect but provides a reasonable estimate
            if 'high_income_household_pct' in df.columns:
                # Scale to create an index from 0-100
                max_value = df['high_income_household_pct'].max()
                if max_value > 0:
                    df['executive_density_proxy'] = df['high_income_household_pct'] / max_value * 100
                    logging.info("Created executive density proxy based on high-income households")
                    return df
        
        logging.warning("Unable to estimate executive density with available data")
        return msa_data.get('combined', pd.DataFrame())
    
    except Exception as e:
        logging.error(f"Error estimating executive density: {e}")
        return msa_data.get('combined', pd.DataFrame())

def create_economic_vitality_index(metrics_df):
    """
    Create a composite Economic Vitality Index.
    
    Args:
        metrics_df (pandas.DataFrame): DataFrame with calculated metrics
    
    Returns:
        pandas.DataFrame: DataFrame with Economic Vitality Index
    """
    logging.info("Creating Economic Vitality Index...")
    try:
        df = metrics_df.copy()
        
        # Define the metrics to use for the index
        vitality_metrics = []
        if 'gdp_cagr' in df.columns:
            vitality_metrics.append('gdp_cagr')
        if 'wealth_industry_concentration' in df.columns:
            vitality_metrics.append('wealth_industry_concentration')
        if 'executive_density_proxy' in df.columns:
            vitality_metrics.append('executive_density_proxy')
        
        if not vitality_metrics:
            logging.warning("No metrics available for Economic Vitality Index")
            return df
        
        # Normalize each metric to a 0-100 scale
        for metric in vitality_metrics:
            # Filter out negative values for some metrics (like gdp_cagr)
            if metric == 'gdp_cagr':
                min_val = max(0, df[metric].min())  # Treat negative growth as 0
            else:
                min_val = df[metric].min()
            
            max_val = df[metric].max()
            
            if max_val > min_val:
                df[f'{metric}_norm'] = 100 * (df[metric] - min_val) / (max_val - min_val)
            else:
                df[f'{metric}_norm'] = 0
        
        # Calculate the Economic Vitality Index as the average of normalized metrics
        norm_metrics = [f'{metric}_norm' for metric in vitality_metrics]
        df['economic_vitality_index'] = df[norm_metrics].mean(axis=1)
        
        logging.info(f"Created Economic Vitality Index using {len(vitality_metrics)} metrics")
        return df
    
    except Exception as e:
        logging.error(f"Error creating Economic Vitality Index: {e}")
        return metrics_df

def create_vitality_ranking(metrics_df):
    """
    Create a ranking of MSAs based on economic vitality.
    
    Args:
        metrics_df (pandas.DataFrame): DataFrame with all calculated metrics
    
    Returns:
        pandas.DataFrame: DataFrame with rankings
    """
    logging.info("Creating economic vitality rankings...")
    try:
        df = metrics_df.copy()
        
        # Define the metrics to use for ranking
        ranking_metrics = []
        if 'economic_vitality_index' in df.columns:
            ranking_metrics.append('economic_vitality_index')
        if 'gdp_cagr' in df.columns:
            ranking_metrics.append('gdp_cagr')
        if 'wealth_industry_concentration' in df.columns:
            ranking_metrics.append('wealth_industry_concentration')
        
        if not ranking_metrics:
            logging.warning("No metrics available for ranking")
            return df
        
        # Create individual rankings for each metric (higher values are better)
        for metric in ranking_metrics:
            df[f'{metric}_rank'] = df[metric].rank(ascending=False, method='min')
        
        # Create a composite rank (average of individual ranks)
        rank_columns = [f'{metric}_rank' for metric in ranking_metrics]
        df['composite_vitality_rank'] = df[rank_columns].mean(axis=1)
        
        # Sort by composite rank
        df = df.sort_values('composite_vitality_rank')
        
        # Add a simple rank column
        df['economic_vitality_rank'] = range(1, len(df) + 1)
        
        logging.info(f"Created economic vitality rankings using {len(ranking_metrics)} metrics")
        return df
    
    except Exception as e:
        logging.error(f"Error creating economic vitality rankings: {e}")
        return metrics_df

def combine_all_metrics(econ_metrics, other_metrics):
    """
    Combine economic vitality metrics with other previously calculated metrics.
    
    Args:
        econ_metrics (pandas.DataFrame): DataFrame with economic vitality metrics
        other_metrics (dict): Dictionary of DataFrames with other metrics
    
    Returns:
        pandas.DataFrame: DataFrame with all metrics combined
    """
    logging.info("Combining all metrics...")
    try:
        # Start with economic vitality metrics
        combined_df = econ_metrics.copy()
        
        # Add HNWI metrics if available
        if 'hnwi' in other_metrics:
            hnwi_df = other_metrics['hnwi']
            
            # Identify HNWI-specific columns to keep
            hnwi_cols = ['hnwi_density_index', 'luxury_home_pct', 'high_income_household_pct', 
                         'deposit_per_capita', 'hnwi_density_rank']
            hnwi_cols = [col for col in hnwi_cols if col in hnwi_df.columns and col not in combined_df.columns]
            
            if hnwi_cols:
                # Merge HNWI metrics
                combined_df = pd.merge(
                    combined_df,
                    hnwi_df[['cbsa_code'] + hnwi_cols],
                    on='cbsa_code',
                    how='left'
                )
                logging.info(f"Added HNWI metrics: {', '.join(hnwi_cols)}")
        
        # Add financial services metrics if available
        if 'fs' in other_metrics:
            fs_df = other_metrics['fs']
            
            # Identify FS-specific columns to keep
            fs_cols = ['advisor_per_10k', 'hnwi_to_advisor_ratio', 'opportunity_score', 
                      'opportunity_level', 'coverage_opportunity_rank']
            fs_cols = [col for col in fs_cols if col in fs_df.columns and col not in combined_df.columns]
            
            if fs_cols:
                # Merge FS metrics
                combined_df = pd.merge(
                    combined_df,
                    fs_df[['cbsa_code'] + fs_cols],
                    on='cbsa_code',
                    how='left'
                )
                logging.info(f"Added financial services metrics: {', '.join(fs_cols)}")
        
        # Calculate a comprehensive market opportunity score if we have all three dimensions
        if all(x in combined_df.columns for x in ['hnwi_density_index', 'opportunity_score', 'economic_vitality_index']):
            # Weight the three dimensions
            combined_df['overall_opportunity_score'] = (
                combined_df['hnwi_density_index'] * 0.4 +  # Wealth concentration weight
                combined_df['opportunity_score'] * 0.4 +   # Market opportunity weight
                combined_df['economic_vitality_index'] * 0.2  # Economic foundation weight
            )
            
            # Rank MSAs by overall opportunity
            combined_df['overall_opportunity_rank'] = combined_df['overall_opportunity_score'].rank(ascending=False, method='min')
            logging.info("Calculated overall market opportunity score and ranking")
        
        return combined_df
    
    except Exception as e:
        logging.error(f"Error combining all metrics: {e}")
        return econ_metrics

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
        metrics_file = os.path.join(OUTPUT_DIR, f"economic_vitality_metrics_{timestamp}.csv")
        metrics_df.to_csv(metrics_file, index=False)
        logging.info(f"Saved economic vitality metrics to {metrics_file}")
        
        # Save a ranked list with key metrics
        key_metrics = ['cbsa_code', 'cbsa_title']
        
        # Add available metrics columns
        for col in ['gdp_cagr', 'wealth_industry_concentration', 'executive_density_proxy', 
                   'economic_vitality_index', 'economic_vitality_rank']:
            if col in metrics_df.columns:
                key_metrics.append(col)
        
        # Filter and sort by rank
        if 'economic_vitality_rank' in metrics_df.columns:
            ranked_df = metrics_df[key_metrics].sort_values('economic_vitality_rank')
            ranked_file = os.path.join(OUTPUT_DIR, f"economic_vitality_rankings_{timestamp}.csv")
            ranked_df.to_csv(ranked_file, index=False)
            logging.info(f"Saved rankings to {ranked_file}")
        
        # Save comprehensive market opportunity ranking if available
        if 'overall_opportunity_score' in metrics_df.columns:
            # Define columns for comprehensive ranking
            comp_cols = ['cbsa_code', 'cbsa_title', 'overall_opportunity_score', 'overall_opportunity_rank']
            
            # Add dimension scores if available
            for col in ['hnwi_density_index', 'opportunity_score', 'economic_vitality_index',
                      'gdp_cagr', 'high_income_household_pct', 'advisor_per_10k']:
                if col in metrics_df.columns:
                    comp_cols.append(col)
            
            # Sort by overall opportunity rank
            comp_df = metrics_df[comp_cols].sort_values('overall_opportunity_rank')
            comp_file = os.path.join(OUTPUT_DIR, f"overall_market_opportunity_{timestamp}.csv")
            comp_df.to_csv(comp_file, index=False)
            logging.info(f"Saved comprehensive market opportunity ranking to {comp_file}")
        
        # Create a documentation file
        doc_content = f"""# Economic Vitality Metrics

This file contains Economic Vitality indicators for Metropolitan Statistical Areas (MSAs).

## Metrics Calculated

1. **GDP Growth Trend**: Compound Annual Growth Rate (CAGR) of MSA GDP.
   - Captures economic momentum and wealth creation potential.

2. **Wealth-Creating Industries**: Concentration of industries associated with wealth creation.
   - Higher values indicate greater presence of financial services, real estate, manufacturing, and professional services.

3. **Executive Density**: Estimated concentration of executives and high-income professionals.
   - Based on proxy measures of high-income households.

4. **Economic Vitality Index**: A composite index (0-100) combining the above metrics.
   - Higher values indicate stronger economic fundamentals for wealth management opportunity.

## Comprehensive Market Opportunity

When combined with HNWI Density and Financial Services Coverage metrics, a comprehensive market opportunity score is calculated that incorporates:

- Wealth concentration (HNWI Density Index)
- Market opportunity (Financial Services Coverage)
- Economic foundation (Economic Vitality Index)

## Files Generated on {timestamp}

- `economic_vitality_metrics_{timestamp}.csv`: Complete economic vitality metrics
- `economic_vitality_rankings_{timestamp}.csv`: Rankings based on economic vitality
- `overall_market_opportunity_{timestamp}.csv`: Comprehensive market opportunity ranking (if available)
- `industry_analysis_detail_{timestamp}.csv`: Detailed industry composition analysis

## Notes

- The "Business Formation Rate" metric requires additional data on new business establishments
- The "Entrepreneur Quotient" metric requires data on business exits and liquidity events
- GDP growth is calculated using the most recent 5 years of available data (or less if 5 years not available)
"""
        
        doc_file = os.path.join(OUTPUT_DIR, "economic_vitality_metrics_documentation.md")
        with open(doc_file, 'w') as f:
            f.write(doc_content)
        logging.info(f"Created documentation file at {doc_file}")
        
    except Exception as e:
        logging.error(f"Error saving results: {e}")

def main():
    """Main function to calculate economic vitality metrics"""
    logging.info("Starting economic vitality metrics calculation...")
    
    # Load MSA data
    msa_data = load_msa_data()
    if not msa_data or 'combined' not in msa_data:
        logging.error("Failed to load required MSA data. Exiting.")
        return
    
    # Load other metrics if available
    other_metrics = load_other_metrics()
    
    # Start with the combined metrics
    metrics_df = msa_data['combined'].copy()
    
    # Calculate each economic vitality metric
    metrics_df = calculate_gdp_growth_trend(msa_data)
    metrics_df = analyze_wealth_creating_industries(msa_data)
    metrics_df = estimate_executive_density(msa_data)
    
    # Create Economic Vitality Index
    metrics_df = create_economic_vitality_index(metrics_df)
    
    # Create rankings
    metrics_df = create_vitality_ranking(metrics_df)
    
    # Combine with other metrics if available
    if other_metrics:
        metrics_df = combine_all_metrics(metrics_df, other_metrics)
    
    # Save results
    save_results(metrics_df)
    
    logging.info("Economic vitality metrics calculation complete!")

if __name__ == "__main__":
    main()