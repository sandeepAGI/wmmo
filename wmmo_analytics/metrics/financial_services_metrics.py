#!/usr/bin/env python3
"""
financial_services_metrics.py

This script calculates Financial Services Coverage metrics for each
Metropolitan Statistical Area (MSA) using the aggregated data. These metrics include:

1. Advisor Penetration Rate: Registered advisors per 10,000 residents
2. HNWI-to-Advisor Ratio: Estimated HNWIs per advisor
3. Wealth Management Saturation: Market share of top firms (requires additional data)
4. Average AUM per Advisor: Average assets under management (requires additional data)
5. Service Mix Alignment: Alignment of advisor specializations (requires additional data)

These metrics help identify underserved markets with potential for wealth management services.
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
        logging.FileHandler("financial_services_metrics.log"),
        logging.StreamHandler()
    ]
)

# Constants
MSA_DATA_DIR = os.path.join(parent_dir, "msadata")
BLS_DATA_DIR = os.path.join(parent_dir, "..", "bls_advisors_data")
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
        
        # Load BLS advisor data (for more detailed analysis)
        bls_files = glob.glob(os.path.join(BLS_DATA_DIR, "*.csv"))
        bls_years = {}
        
        for file_path in bls_files:
            try:
                year = os.path.basename(file_path).replace(".csv", "")
                if year.isdigit():
                    bls_years[year] = file_path
            except:
                continue
        
        if bls_years:
            # Load each year's data
            for year, file_path in sorted(bls_years.items(), key=lambda x: x[0], reverse=True):
                logging.info(f"Loading BLS {year} data from {file_path}")
                msa_data[f'bls_{year}'] = pd.read_csv(file_path)
        
        # Load MSA reference data if available
        reference_files = glob.glob(os.path.join(MSA_DATA_DIR, "msa_reference_*.csv"))
        if reference_files:
            most_recent = sorted(reference_files)[-1]
            logging.info(f"Loading MSA reference data from {most_recent}")
            msa_data['reference'] = pd.read_csv(most_recent)
        
        logging.info(f"Loaded MSA data: {', '.join(msa_data.keys())}")
        return msa_data
    
    except Exception as e:
        logging.error(f"Error loading MSA data: {e}")
        return {}

def load_hnwi_metrics():
    """
    Load previously calculated HNWI density metrics if available.
    
    Returns:
        pandas.DataFrame or None: HNWI metrics DataFrame or None if not available
    """
    logging.info("Checking for HNWI density metrics...")
    try:
        # Look for HNWI metrics file
        hnwi_files = glob.glob(os.path.join(OUTPUT_DIR, "hnwi_density_metrics_*.csv"))
        if hnwi_files:
            most_recent = sorted(hnwi_files)[-1]
            logging.info(f"Loading HNWI metrics from {most_recent}")
            return pd.read_csv(most_recent)
        else:
            logging.warning("No HNWI metrics file found")
            return None
    
    except Exception as e:
        logging.error(f"Error loading HNWI metrics: {e}")
        return None

def calculate_advisor_penetration_rate(msa_data):
    """
    Calculate Advisor Penetration Rate (advisors per 10,000 residents).
    
    Args:
        msa_data (dict): Dictionary of MSA data DataFrames
    
    Returns:
        pandas.DataFrame: DataFrame with advisor penetration metrics
    """
    logging.info("Calculating Advisor Penetration Rate...")
    try:
        # Check if we already have advisor_per_10k in combined data
        if 'combined' in msa_data and 'advisor_per_10k' in msa_data['combined'].columns:
            logging.info("Using pre-calculated Advisor Penetration Rate")
            return msa_data['combined']
        
        # If not available in combined data, use the most recent BLS data
        bls_keys = [k for k in msa_data.keys() if k.startswith('bls_')]
        if not bls_keys:
            logging.warning("No BLS advisor data available")
            return msa_data.get('combined', pd.DataFrame())
        
        most_recent_key = sorted(bls_keys)[-1]
        bls_df = msa_data[most_recent_key].copy()
        
        # Check if we have the necessary columns
        if 'tot_emp' in bls_df.columns:
            # For BLS data, we need to map area codes to CBSA codes
            # If there's a direct CBSA field, use it
            cbsa_col = None
            if 'cbsa' in bls_df.columns:
                cbsa_col = 'cbsa'
            elif 'CBSA' in bls_df.columns:
                cbsa_col = 'CBSA'
            
            if cbsa_col:
                # Group by CBSA and sum advisor counts
                advisor_counts = bls_df.groupby(cbsa_col).agg({
                    'tot_emp': 'sum',
                    'area_title': 'first'  # Keep the area name
                }).reset_index()
                
                advisor_counts.rename(columns={
                    cbsa_col: 'cbsa_code',
                    'tot_emp': 'total_advisors',
                    'area_title': 'area_name'
                }, inplace=True)
                
                # If we have combined data with population, calculate penetration rate
                if 'combined' in msa_data and 'total_population' in msa_data['combined'].columns:
                    # Merge advisor counts with combined data
                    result = pd.merge(
                        msa_data['combined'],
                        advisor_counts[['cbsa_code', 'total_advisors']],
                        on='cbsa_code',
                        how='left'
                    )
                    
                    # Calculate advisors per 10,000 residents
                    mask = (result['total_population'].notna()) & (result['total_population'] > 0) & (result['total_advisors'].notna())
                    result.loc[mask, 'advisor_per_10k'] = result.loc[mask, 'total_advisors'] / result.loc[mask, 'total_population'] * 10000
                    
                    logging.info(f"Calculated Advisor Penetration Rate for {mask.sum()} MSAs")
                    return result
                else:
                    logging.warning("No population data available to calculate advisor penetration rate")
            else:
                logging.warning("BLS data doesn't have CBSA code column")
        else:
            logging.warning("BLS data doesn't have advisor count column")
        
        logging.warning("Unable to calculate Advisor Penetration Rate")
        return msa_data.get('combined', pd.DataFrame())
    
    except Exception as e:
        logging.error(f"Error calculating Advisor Penetration Rate: {e}")
        return msa_data.get('combined', pd.DataFrame())

def calculate_hnwi_advisor_ratio(msa_data):
    """
    Calculate HNWI-to-Advisor Ratio (estimated HNWIs per advisor).
    
    Args:
        msa_data (dict): Dictionary of MSA data DataFrames
    
    Returns:
        pandas.DataFrame: DataFrame with HNWI-to-advisor metrics
    """
    logging.info("Calculating HNWI-to-Advisor Ratio...")
    try:
        # Check if we already have hnwi_to_advisor_ratio in combined data
        if 'combined' in msa_data and 'hnwi_to_advisor_ratio' in msa_data['combined'].columns:
            logging.info("Using pre-calculated HNWI-to-Advisor Ratio")
            return msa_data['combined']
        
        combined_df = msa_data.get('combined', None)
        if combined_df is None:
            logging.warning("No combined data available")
            return pd.DataFrame()
        
        # Check if we have the necessary columns
        if all(col in combined_df.columns for col in ['high_income_household_pct', 'total_households', 'total_advisors']):
            # Make a copy of the combined data
            result = combined_df.copy()
            
            # Estimate number of high-income households (as proxy for HNWIs)
            mask = (result['high_income_household_pct'].notna()) & (result['total_households'].notna())
            result.loc[mask, 'high_income_households'] = result.loc[mask, 'high_income_household_pct'] * result.loc[mask, 'total_households'] / 100
            
            # Calculate HNWI-to-advisor ratio
            mask = (result['high_income_households'].notna()) & (result['total_advisors'].notna()) & (result['total_advisors'] > 0)
            result.loc[mask, 'hnwi_to_advisor_ratio'] = result.loc[mask, 'high_income_households'] / result.loc[mask, 'total_advisors']
            
            logging.info(f"Calculated HNWI-to-Advisor Ratio for {mask.sum()} MSAs")
            return result
        else:
            missing = [col for col in ['high_income_household_pct', 'total_households', 'total_advisors'] if col not in combined_df.columns]
            logging.warning(f"Missing required columns for HNWI-to-Advisor Ratio: {missing}")
            return combined_df
    
    except Exception as e:
        logging.error(f"Error calculating HNWI-to-Advisor Ratio: {e}")
        return msa_data.get('combined', pd.DataFrame())

def estimate_market_opportunity(metrics_df):
    """
    Estimate market opportunity based on HNWI density and advisor coverage.
    
    Args:
        metrics_df (pandas.DataFrame): DataFrame with calculated metrics
    
    Returns:
        pandas.DataFrame: DataFrame with market opportunity metrics
    """
    logging.info("Estimating market opportunity...")
    try:
        # Make a copy of the metrics data
        df = metrics_df.copy()
        
        # Define weights for opportunity scoring
        opportunity_weights = {
            'hnwi_density_index': 0.25,        # Higher HNWI density is better
            'high_income_household_pct': 0.15,  # Higher % of high-income households is better
            'gdp_cagr': 0.15,                   # Higher GDP growth is better
            'advisor_per_10k': -0.25,           # Lower advisor penetration is better (underserved)
            'hnwi_to_advisor_ratio': 0.20       # Higher HNWI-to-advisor ratio is better (underserved)
        }
        
        # Create normalized scores for each metric (0-100 scale)
        for metric, weight in opportunity_weights.items():
            if metric in df.columns:
                # Handle positive and negative weights differently
                if weight > 0:
                    # Higher values are better
                    min_val = df[metric].min()
                    max_val = df[metric].max()
                    if max_val > min_val:
                        df[f'{metric}_score'] = 100 * (df[metric] - min_val) / (max_val - min_val)
                    else:
                        df[f'{metric}_score'] = 0
                else:
                    # Lower values are better
                    min_val = df[metric].min()
                    max_val = df[metric].max()
                    if max_val > min_val:
                        df[f'{metric}_score'] = 100 * (max_val - df[metric]) / (max_val - min_val)
                    else:
                        df[f'{metric}_score'] = 0
        
        # Calculate the opportunity score as weighted average of normalized scores
        score_columns = []
        weights = []
        
        for metric, weight in opportunity_weights.items():
            score_col = f'{metric}_score'
            if score_col in df.columns:
                score_columns.append(score_col)
                weights.append(abs(weight))  # Use absolute weight for weighted average
        
        if score_columns:
            # Calculate weighted average
            df['opportunity_score'] = 0
            for i, col in enumerate(score_columns):
                df['opportunity_score'] += df[col] * weights[i] / sum(weights)
            
            # Categorize opportunity
            df['opportunity_level'] = pd.cut(
                df['opportunity_score'],
                bins=[0, 25, 50, 75, 100],
                labels=['Low', 'Moderate', 'High', 'Very High']
            )
            
            logging.info(f"Calculated opportunity scores using {len(score_columns)} metrics")
        else:
            logging.warning("No metrics available for opportunity scoring")
        
        return df
    
    except Exception as e:
        logging.error(f"Error estimating market opportunity: {e}")
        return metrics_df

def create_coverage_ranking(metrics_df):
    """
    Create a ranking of MSAs based on financial services coverage metrics.
    
    Args:
        metrics_df (pandas.DataFrame): DataFrame with all calculated metrics
    
    Returns:
        pandas.DataFrame: DataFrame with rankings
    """
    logging.info("Creating financial services coverage rankings...")
    try:
        df = metrics_df.copy()
        
        # Define the metrics to use for ranking
        ranking_metrics = []
        if 'advisor_per_10k' in df.columns:
            ranking_metrics.append('advisor_per_10k')
        if 'hnwi_to_advisor_ratio' in df.columns:
            ranking_metrics.append('hnwi_to_advisor_ratio')
        if 'opportunity_score' in df.columns:
            ranking_metrics.append('opportunity_score')
        
        if not ranking_metrics:
            logging.warning("No metrics available for ranking")
            return df
        
        # Create individual rankings for each metric
        # For advisor_per_10k, lower is better (ascending=True)
        # For hnwi_to_advisor_ratio, higher is better (ascending=False)
        # For opportunity_score, higher is better (ascending=False)
        
        if 'advisor_per_10k' in ranking_metrics:
            df['advisor_per_10k_rank'] = df['advisor_per_10k'].rank(ascending=True, method='min')
        
        if 'hnwi_to_advisor_ratio' in ranking_metrics:
            df['hnwi_to_advisor_ratio_rank'] = df['hnwi_to_advisor_ratio'].rank(ascending=False, method='min')
        
        if 'opportunity_score' in ranking_metrics:
            df['opportunity_score_rank'] = df['opportunity_score'].rank(ascending=False, method='min')
        
        # Create a composite rank (average of individual ranks)
        rank_columns = [f'{metric}_rank' for metric in ranking_metrics if f'{metric}_rank' in df.columns]
        if rank_columns:
            df['composite_coverage_rank'] = df[rank_columns].mean(axis=1)
            
            # Sort by composite rank
            df = df.sort_values('composite_coverage_rank')
            
            # Add a simple rank column
            df['coverage_opportunity_rank'] = range(1, len(df) + 1)
            
            logging.info(f"Created coverage rankings using {len(rank_columns)} metrics")
        
        return df
    
    except Exception as e:
        logging.error(f"Error creating coverage rankings: {e}")
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
        metrics_file = os.path.join(OUTPUT_DIR, f"financial_services_metrics_{timestamp}.csv")
        metrics_df.to_csv(metrics_file, index=False)
        logging.info(f"Saved complete metrics to {metrics_file}")
        
        # Save a ranked list with key metrics
        key_metrics = ['cbsa_code', 'cbsa_title']
        
        # Add available metrics columns
        for col in ['advisor_per_10k', 'hnwi_to_advisor_ratio', 'high_income_household_pct', 
                    'opportunity_score', 'opportunity_level', 'coverage_opportunity_rank']:
            if col in metrics_df.columns:
                key_metrics.append(col)
        
        # Filter and sort by rank
        if 'coverage_opportunity_rank' in metrics_df.columns:
            ranked_df = metrics_df[key_metrics].sort_values('coverage_opportunity_rank')
            ranked_file = os.path.join(OUTPUT_DIR, f"coverage_opportunity_rankings_{timestamp}.csv")
            ranked_df.to_csv(ranked_file, index=False)
            logging.info(f"Saved rankings to {ranked_file}")
        
        # Also create a top opportunities file with more detail
        if 'opportunity_score' in metrics_df.columns:
            # Filter for top opportunities (e.g., top 20% or score > 75)
            top_df = metrics_df.nlargest(int(len(metrics_df) * 0.2), 'opportunity_score')
            
            # Include more details for analysis
            detail_cols = key_metrics + [
                'total_population', 'total_advisors', 
                'high_income_households', 'gdp_cagr',
                'luxury_home_pct', 'hnwi_density_index'
            ]
            detail_cols = [col for col in detail_cols if col in metrics_df.columns]
            
            top_file = os.path.join(OUTPUT_DIR, f"top_market_opportunities_{timestamp}.csv")
            top_df[detail_cols].to_csv(top_file, index=False)
            logging.info(f"Saved top market opportunities to {top_file}")
        
        # Create a documentation file
        doc_content = f"""# Financial Services Coverage Metrics

This file contains Financial Services Coverage metrics for Metropolitan Statistical Areas (MSAs).

## Metrics Calculated

1. **Advisor Penetration Rate**: Number of registered financial advisors per 10,000 residents.
   - Lower values may indicate underserved markets.

2. **HNWI-to-Advisor Ratio**: Estimated number of high-net-worth individuals per advisor.
   - Higher values may indicate greater opportunity.

3. **Opportunity Score**: A composite score (0-100) indicating the market opportunity for wealth management services.
   - Based on HNWI density, advisor penetration, and market growth.
   - Higher values indicate greater opportunity.

4. **Opportunity Level**: Categorization of opportunity as Low, Moderate, High, or Very High.
   - Based on opportunity score quartiles.

## Rankings

The MSAs are ranked based on a composite of the coverage metrics. Lower ranks indicate greater opportunity (underserved markets with high wealth concentration).

## Files Generated on {timestamp}

- `financial_services_metrics_{timestamp}.csv`: Complete metrics for all MSAs
- `coverage_opportunity_rankings_{timestamp}.csv`: Simplified rankings with key metrics
- `top_market_opportunities_{timestamp}.csv`: Detailed profile of top market opportunities

## Notes

- Data is derived from BLS advisor statistics and Census wealth indicators
- The "Wealth Management Saturation" metric requires additional data on market share of top firms
- The "Average AUM per Advisor" metric requires SEC data not currently available
- The "Service Mix Alignment" metric requires data on advisor specializations not currently available
"""
        
        doc_file = os.path.join(OUTPUT_DIR, "financial_services_metrics_documentation.md")
        with open(doc_file, 'w') as f:
            f.write(doc_content)
        logging.info(f"Created documentation file at {doc_file}")
        
    except Exception as e:
        logging.error(f"Error saving results: {e}")

def main():
    """Main function to calculate financial services coverage metrics"""
    logging.info("Starting financial services coverage metrics calculation...")
    
    # Load MSA data
    msa_data = load_msa_data()
    if not msa_data or 'combined' not in msa_data:
        logging.error("Failed to load required MSA data. Exiting.")
        return
    
    # Check for HNWI metrics
    hnwi_metrics = load_hnwi_metrics()
    if hnwi_metrics is not None:
        # Use HNWI metrics as the base
        metrics_df = hnwi_metrics.copy()
        logging.info("Using HNWI metrics as base for calculations")
    else:
        # Start with the combined metrics
        metrics_df = msa_data['combined'].copy()
        logging.info("Using combined metrics as base for calculations")
    
    # Calculate advisor penetration rate
    metrics_df = calculate_advisor_penetration_rate(msa_data)
    
    # Calculate HNWI-to-advisor ratio
    metrics_df = calculate_hnwi_advisor_ratio(msa_data)
    
    # Estimate market opportunity
    metrics_df = estimate_market_opportunity(metrics_df)
    
    # Create rankings
    metrics_df = create_coverage_ranking(metrics_df)
    
    # Save results
    save_results(metrics_df)
    
    logging.info("Financial services coverage metrics calculation complete!")

if __name__ == "__main__":
    main()