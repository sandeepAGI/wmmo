#!/usr/bin/env python3
"""
identify_underserved_markets.py

This script identifies underserved wealth management markets - areas with high
potential (high wealth concentration and strong economic fundamentals) but low
advisor coverage. It generates reports and visualizations highlighting the top
15 underserved markets for wealth management opportunities.
"""

import os
import sys
import pandas as pd
import numpy as np
import glob
import logging
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("underserved_markets.log"),
        logging.StreamHandler()
    ]
)

# Constants
METRICS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "metrics")
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

def load_metrics_data():
    """
    Load metrics data from the most recent files.
    
    Returns:
        dict: Dictionary containing DataFrames of metrics data
    """
    logging.info("Loading metrics data...")
    data = {}
    
    try:
        # Look for the most recent files of each type
        # First, try the comprehensive market opportunity file if it exists
        overall_files = glob.glob(os.path.join(METRICS_DIR, "overall_market_opportunity_*.csv"))
        if overall_files:
            most_recent = sorted(overall_files)[-1]
            logging.info(f"Loading overall market opportunity data from {most_recent}")
            data['overall'] = pd.read_csv(most_recent)
            return data  # If we have the overall file, we don't need other files
        
        # If no overall file, try loading individual metrics files
        # HNWI density metrics
        hnwi_files = glob.glob(os.path.join(METRICS_DIR, "hnwi_density_metrics_*.csv"))
        if hnwi_files:
            most_recent = sorted(hnwi_files)[-1]
            logging.info(f"Loading HNWI density metrics from {most_recent}")
            data['hnwi'] = pd.read_csv(most_recent)
        
        # Financial services metrics
        fs_files = glob.glob(os.path.join(METRICS_DIR, "financial_services_metrics_*.csv"))
        if fs_files:
            most_recent = sorted(fs_files)[-1]
            logging.info(f"Loading financial services metrics from {most_recent}")
            data['fs'] = pd.read_csv(most_recent)
        
        # Economic vitality metrics
        econ_files = glob.glob(os.path.join(METRICS_DIR, "economic_vitality_metrics_*.csv"))
        if econ_files:
            most_recent = sorted(econ_files)[-1]
            logging.info(f"Loading economic vitality metrics from {most_recent}")
            data['econ'] = pd.read_csv(most_recent)
        
        # If we don't have at least HNWI and FS metrics, we can't identify underserved markets
        if not ('hnwi' in data and 'fs' in data):
            logging.error("Missing required metrics data. Need at least HNWI and FS metrics.")
            return None
        
        # Combine metrics if we have separate files
        # Start with HNWI metrics
        combined = data['hnwi']
        
        # Add financial services metrics
        if 'fs' in data:
            # Get columns that are in FS but not in HNWI (to avoid duplicates)
            fs_unique_cols = [col for col in data['fs'].columns if col not in combined.columns or col == 'cbsa_code']
            
            # Merge with combined data
            combined = pd.merge(
                combined,
                data['fs'][fs_unique_cols],
                on='cbsa_code',
                how='inner'
            )
        
        # Add economic vitality metrics if available
        if 'econ' in data:
            # Get columns that are in ECON but not in combined (to avoid duplicates)
            econ_unique_cols = [col for col in data['econ'].columns if col not in combined.columns or col == 'cbsa_code']
            
            # Merge with combined data
            combined = pd.merge(
                combined,
                data['econ'][econ_unique_cols],
                on='cbsa_code',
                how='inner'
            )
        
        data['combined'] = combined
        logging.info(f"Created combined metrics dataset with {len(combined)} MSAs")
        
        return data
    
    except Exception as e:
        logging.error(f"Error loading metrics data: {e}")
        return None

def identify_underserved_markets(data, n_markets=15):
    """
    Identify underserved markets based on high potential and low advisor coverage.
    
    Args:
        data (dict): Dictionary containing DataFrames of metrics data
        n_markets (int): Number of top underserved markets to identify
    
    Returns:
        pandas.DataFrame: DataFrame of top underserved markets
    """
    logging.info(f"Identifying top {n_markets} underserved markets...")
    
    try:
        # Determine which data to use
        if 'overall' in data:
            df = data['overall'].copy()
            
            # Check if we have all the necessary columns
            required_cols = ['cbsa_code', 'cbsa_title', 'overall_opportunity_score', 'advisor_per_10k']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                logging.warning(f"Missing required columns in overall data: {missing_cols}")
                
                # Try using the combined data instead
                if 'combined' in data:
                    df = data['combined'].copy()
                else:
                    logging.error("No suitable data available for identifying underserved markets")
                    return None
        elif 'combined' in data:
            df = data['combined'].copy()
        else:
            logging.error("No suitable data available for identifying underserved markets")
            return None
        
        # To identify underserved markets, we need:
        # 1. Wealth potential (HNWI density)
        # 2. Advisor coverage (advisor penetration)
        
        # Check if we have the necessary metrics
        wealth_metrics = []
        if 'hnwi_density_index' in df.columns:
            wealth_metrics.append('hnwi_density_index')
        if 'high_income_household_pct' in df.columns:
            wealth_metrics.append('high_income_household_pct')
        if 'luxury_home_pct' in df.columns:
            wealth_metrics.append('luxury_home_pct')
        
        if not wealth_metrics:
            logging.error("No wealth metrics available")
            return None
        
        # Check for advisor coverage metrics
        if 'advisor_per_10k' not in df.columns:
            logging.error("Advisor coverage metric not available")
            return None
        
        # Add economic growth metrics if available
        growth_metrics = []
        if 'gdp_cagr' in df.columns:
            growth_metrics.append('gdp_cagr')
        if 'economic_vitality_index' in df.columns:
            growth_metrics.append('economic_vitality_index')
        
        # Create a composite potential score based on wealth and growth metrics
        logging.info(f"Using wealth metrics: {', '.join(wealth_metrics)}")
        
        # Normalize each wealth metric to 0-100 scale
        for metric in wealth_metrics:
            min_val = df[metric].min()
            max_val = df[metric].max()
            if max_val > min_val:
                df[f'{metric}_norm'] = 100 * (df[metric] - min_val) / (max_val - min_val)
            else:
                df[f'{metric}_norm'] = 0
        
        # Calculate composite wealth potential score
        wealth_norm_metrics = [f'{metric}_norm' for metric in wealth_metrics]
        df['wealth_potential_score'] = df[wealth_norm_metrics].mean(axis=1)
        
        # Add growth potential if available
        if growth_metrics:
            logging.info(f"Using growth metrics: {', '.join(growth_metrics)}")
            
            # Normalize each growth metric to 0-100 scale
            for metric in growth_metrics:
                # For gdp_cagr, only normalize positive values
                if metric == 'gdp_cagr':
                    min_val = max(0, df[metric].min())  # Treat negative growth as 0
                else:
                    min_val = df[metric].min()
                
                max_val = df[metric].max()
                if max_val > min_val:
                    df[f'{metric}_norm'] = 100 * (df[metric] - min_val) / (max_val - min_val)
                else:
                    df[f'{metric}_norm'] = 0
            
            # Calculate growth potential score
            growth_norm_metrics = [f'{metric}_norm' for metric in growth_metrics]
            df['growth_potential_score'] = df[growth_norm_metrics].mean(axis=1)
            
            # Calculate overall potential score (70% wealth, 30% growth)
            df['market_potential_score'] = df['wealth_potential_score'] * 0.7 + df['growth_potential_score'] * 0.3
        else:
            # If no growth metrics, use wealth potential as the overall potential
            df['market_potential_score'] = df['wealth_potential_score']
        
        # Normalize advisor coverage metric (lower is better for underserved)
        min_val = df['advisor_per_10k'].min()
        max_val = df['advisor_per_10k'].max()
        if max_val > min_val:
            df['advisor_coverage_norm'] = 100 * (max_val - df['advisor_per_10k']) / (max_val - min_val)
        else:
            df['advisor_coverage_norm'] = 0
        
        # Calculate underserved market score
        # Higher weight on potential (60%) and lower on coverage (40%)
        df['underserved_score'] = df['market_potential_score'] * 0.6 + df['advisor_coverage_norm'] * 0.4
        
        # Rank markets by underserved score
        df['underserved_rank'] = df['underserved_score'].rank(ascending=False, method='min')
        
        # Classify market status
        df['market_status'] = 'Balanced'
        
        # Markets with high potential (>60) and low coverage (<40) are "Underserved"
        df.loc[(df['market_potential_score'] > 60) & (df['advisor_coverage_norm'] > 60), 'market_status'] = 'Underserved'
        
        # Markets with high potential (>60) and high coverage (>60) are "Competitive"
        df.loc[(df['market_potential_score'] > 60) & (df['advisor_coverage_norm'] < 40), 'market_status'] = 'Competitive'
        
        # Markets with low potential (<40) and low coverage (<40) are "Low Opportunity"
        df.loc[(df['market_potential_score'] < 40) & (df['advisor_coverage_norm'] < 40), 'market_status'] = 'Low Opportunity'
        
        # Markets with low potential (<40) and high coverage (>60) are "Oversaturated"
        df.loc[(df['market_potential_score'] < 40) & (df['advisor_coverage_norm'] > 60), 'market_status'] = 'Oversaturated'
        
        # Select top underserved markets
        underserved = df.sort_values('underserved_score', ascending=False).head(n_markets)
        
        logging.info(f"Identified {len(underserved)} underserved markets")
        return df, underserved
    
    except Exception as e:
        logging.error(f"Error identifying underserved markets: {e}")
        return None, None

def create_visualizations(df, underserved):
    """
    Create visualizations of underserved markets.
    
    Args:
        df (pandas.DataFrame): DataFrame with all markets data
        underserved (pandas.DataFrame): DataFrame of top underserved markets
    
    Returns:
        list: List of paths to generated visualization files
    """
    logging.info("Creating visualizations...")
    vis_files = []
    
    try:
        # Set the style
        sns.set(style="whitegrid")
        
        # 1. Create a scatter plot of all markets
        plt.figure(figsize=(12, 8))
        
        # Define colors for market status
        status_colors = {
            'Underserved': 'green',
            'Competitive': 'red',
            'Low Opportunity': 'gray',
            'Oversaturated': 'orange',
            'Balanced': 'blue'
        }
        
        # Plot all points
        scatter = sns.scatterplot(
            data=df,
            x='advisor_per_10k',
            y='market_potential_score',
            hue='market_status',
            palette=status_colors,
            size='total_population' if 'total_population' in df.columns else None,
            sizes=(20, 200) if 'total_population' in df.columns else None,
            alpha=0.6
        )
        
        # Highlight top underserved markets
        for idx, row in underserved.iterrows():
            plt.annotate(
                row['cbsa_title'],
                xy=(row['advisor_per_10k'], row['market_potential_score']),
                xytext=(5, 5),
                textcoords='offset points',
                ha='left',
                va='bottom',
                fontsize=8,
                fontweight='bold'
            )
        
        # Add quadrant lines
        plt.axhline(y=50, color='gray', linestyle='--', alpha=0.5)
        plt.axvline(x=df['advisor_per_10k'].median(), color='gray', linestyle='--', alpha=0.5)
        
        # Add labels for quadrants
        plt.text(
            df['advisor_per_10k'].max() * 0.9, 
            75, 
            'Competitive\n(High potential, High coverage)', 
            ha='right', 
            fontsize=10
        )
        plt.text(
            df['advisor_per_10k'].min() * 1.1, 
            75, 
            'Underserved\n(High potential, Low coverage)', 
            ha='left', 
            fontsize=10
        )
        plt.text(
            df['advisor_per_10k'].max() * 0.9, 
            25, 
            'Oversaturated\n(Low potential, High coverage)', 
            ha='right', 
            fontsize=10
        )
        plt.text(
            df['advisor_per_10k'].min() * 1.1, 
            25, 
            'Low Opportunity\n(Low potential, Low coverage)', 
            ha='left', 
            fontsize=10
        )
        
        # Set titles and labels
        plt.title('Wealth Management Market Opportunity Map', fontsize=16, fontweight='bold')
        plt.xlabel('Advisor Penetration (advisors per 10,000 residents)', fontsize=12)
        plt.ylabel('Market Potential Score', fontsize=12)
        
        # Adjust legend
        plt.legend(title='Market Status', bbox_to_anchor=(1.05, 1), loc='upper left')
        
        # Save the figure
        timestamp = datetime.now().strftime("%Y%m%d")
        scatter_path = os.path.join(OUTPUT_DIR, f"market_opportunity_map_{timestamp}.png")
        plt.tight_layout()
        plt.savefig(scatter_path, dpi=300)
        plt.close()
        
        vis_files.append(scatter_path)
        logging.info(f"Created market opportunity map: {scatter_path}")
        
        # 2. Create a bar chart of top underserved markets
        plt.figure(figsize=(12, 8))
        
        # Sort by underserved score
        top_markets = underserved.sort_values('underserved_score', ascending=True).tail(15)
        
        # Plot horizontal bar chart
        sns.barplot(
            x='underserved_score',
            y='cbsa_title',
            data=top_markets,
            color='green'
        )
        
        # Set titles and labels
        plt.title('Top 15 Underserved Wealth Management Markets', fontsize=16, fontweight='bold')
        plt.xlabel('Underserved Market Score', fontsize=12)
        plt.ylabel('Metropolitan Statistical Area', fontsize=12)
        
        # Add values to bars
        for i, v in enumerate(top_markets['underserved_score']):
            plt.text(v + 1, i, f"{v:.1f}", va='center')
        
        # Save the figure
        bar_path = os.path.join(OUTPUT_DIR, f"top_underserved_markets_{timestamp}.png")
        plt.tight_layout()
        plt.savefig(bar_path, dpi=300)
        plt.close()
        
        vis_files.append(bar_path)
        logging.info(f"Created top underserved markets bar chart: {bar_path}")
        
        # 3. Create a bubble chart showing potential vs. coverage vs. population
        if 'total_population' in underserved.columns:
            plt.figure(figsize=(12, 8))
            
            # Create bubble chart
            sns.scatterplot(
                data=underserved,
                x='advisor_per_10k',
                y='market_potential_score',
                size='total_population',
                sizes=(100, 2000),
                hue='gdp_cagr' if 'gdp_cagr' in underserved.columns else None,
                palette='viridis' if 'gdp_cagr' in underserved.columns else None,
                alpha=0.7
            )
            
            # Add market names
            for idx, row in underserved.iterrows():
                plt.annotate(
                    row['cbsa_title'],
                    xy=(row['advisor_per_10k'], row['market_potential_score']),
                    xytext=(5, 5),
                    textcoords='offset points',
                    ha='left',
                    va='bottom',
                    fontsize=10
                )
            
            # Set titles and labels
            plt.title('Top Underserved Markets: Size, Potential, and Coverage', fontsize=16, fontweight='bold')
            plt.xlabel('Advisor Penetration (advisors per 10,000 residents)', fontsize=12)
            plt.ylabel('Market Potential Score', fontsize=12)
            
            if 'gdp_cagr' in underserved.columns:
                plt.legend(title='GDP Growth Rate (%)', bbox_to_anchor=(1.05, 1), loc='upper left')
            
            # Save the figure
            bubble_path = os.path.join(OUTPUT_DIR, f"underserved_markets_bubble_{timestamp}.png")
            plt.tight_layout()
            plt.savefig(bubble_path, dpi=300)
            plt.close()
            
            vis_files.append(bubble_path)
            logging.info(f"Created underserved markets bubble chart: {bubble_path}")
        
        return vis_files
    
    except Exception as e:
        logging.error(f"Error creating visualizations: {e}")
        return vis_files

def create_report(df, underserved, vis_files):
    """
    Create a comprehensive report on underserved markets.
    
    Args:
        df (pandas.DataFrame): DataFrame with all markets data
        underserved (pandas.DataFrame): DataFrame of top underserved markets
        vis_files (list): List of paths to visualization files
    
    Returns:
        str: Path to the generated report file
    """
    logging.info("Creating underserved markets report...")
    
    try:
        timestamp = datetime.now().strftime("%Y%m%d")
        report_path = os.path.join(OUTPUT_DIR, f"underserved_markets_report_{timestamp}.md")
        
        with open(report_path, 'w') as f:
            f.write("# Top Underserved Wealth Management Markets\n\n")
            f.write(f"*Generated on {datetime.now().strftime('%Y-%m-%d')}*\n\n")
            
            f.write("## Executive Summary\n\n")
            f.write("This report identifies the top 15 underserved wealth management markets in the United States. ")
            f.write("These markets demonstrate high wealth concentration and strong economic fundamentals, but have ")
            f.write("relatively low financial advisor coverage, indicating significant business opportunity.\n\n")
            
            # Add images if available
            if vis_files:
                f.write("## Market Opportunity Map\n\n")
                for vis_file in vis_files:
                    vis_name = os.path.basename(vis_file)
                    f.write(f"![{vis_name}]({vis_name})\n\n")
            
            # Key metrics table
            f.write("## Top 15 Underserved Markets\n\n")
            
            # Define columns to include in the report
            cols_to_report = ['cbsa_code', 'cbsa_title', 'underserved_score', 'market_potential_score', 
                             'advisor_per_10k', 'wealth_potential_score']
            
            # Add additional columns if available
            optional_cols = ['total_population', 'high_income_household_pct', 'gdp_cagr', 
                           'luxury_home_pct', 'deposit_per_capita']
            
            for col in optional_cols:
                if col in underserved.columns:
                    cols_to_report.append(col)
            
            # Get only the columns that exist in the dataframe
            cols_to_report = [col for col in cols_to_report if col in underserved.columns]
            
            # Create a markdown table with the top markets
            f.write("| Rank | MSA | Market Score | Potential | Advisors per 10k ")
            
            # Add optional column headers
            if 'total_population' in cols_to_report:
                f.write("| Population ")
            if 'high_income_household_pct' in cols_to_report:
                f.write("| High Income % ")
            if 'gdp_cagr' in cols_to_report:
                f.write("| GDP Growth % ")
            if 'luxury_home_pct' in cols_to_report:
                f.write("| Luxury Homes % ")
            if 'deposit_per_capita' in cols_to_report:
                f.write("| Deposits per Capita ($) ")
            
            f.write("|\n")
            
            # Add table separator
            f.write("|" + "---|" * (5 + sum([1 for col in optional_cols if col in cols_to_report])) + "\n")
            
            # Add table rows
            rank = 1
            for _, row in underserved.iterrows():
                f.write(f"| {rank} | {row['cbsa_title']} | {row['underserved_score']:.1f} | {row['market_potential_score']:.1f} | {row['advisor_per_10k']:.2f} ")
                
                # Add optional column values
                if 'total_population' in cols_to_report:
                    f.write(f"| {row['total_population']:,.0f} ")
                if 'high_income_household_pct' in cols_to_report:
                    f.write(f"| {row['high_income_household_pct']:.2f}% ")
                if 'gdp_cagr' in cols_to_report:
                    f.write(f"| {row['gdp_cagr']*100:.2f}% ")
                if 'luxury_home_pct' in cols_to_report:
                    f.write(f"| {row['luxury_home_pct']:.2f}% ")
                if 'deposit_per_capita' in cols_to_report:
                    f.write(f"| ${row['deposit_per_capita']:,.0f} ")
                
                f.write("|\n")
                rank += 1
            
            # Market profiles
            f.write("\n## Market Profiles\n\n")
            
            rank = 1
            for _, row in underserved.head(5).iterrows():
                f.write(f"### {rank}. {row['cbsa_title']}\n\n")
                f.write(f"**Underserved Score:** {row['underserved_score']:.1f}  \n")
                f.write(f"**Market Potential:** {row['market_potential_score']:.1f}  \n")
                f.write(f"**Advisor Penetration:** {row['advisor_per_10k']:.2f} per 10,000 residents  \n")
                
                if 'total_population' in row:
                    f.write(f"**Population:** {row['total_population']:,.0f}  \n")
                if 'high_income_household_pct' in row:
                    f.write(f"**High-Income Households:** {row['high_income_household_pct']:.2f}%  \n")
                if 'gdp_cagr' in row:
                    f.write(f"**GDP Growth Rate:** {row['gdp_cagr']*100:.2f}%  \n")
                if 'luxury_home_pct' in row:
                    f.write(f"**Luxury Homes Percentage:** {row['luxury_home_pct']:.2f}%  \n")
                if 'deposit_per_capita' in row:
                    f.write(f"**Banking Deposits per Capita:** ${row['deposit_per_capita']:,.0f}  \n")
                
                f.write("\n**Opportunity Summary:**  \n")
                f.write(f"{row['cbsa_title']} represents a significant wealth management opportunity with ")
                f.write(f"a high concentration of wealth (")
                
                wealth_indicators = []
                if 'high_income_household_pct' in row:
                    wealth_indicators.append(f"{row['high_income_household_pct']:.1f}% high-income households")
                if 'luxury_home_pct' in row:
                    wealth_indicators.append(f"{row['luxury_home_pct']:.1f}% luxury homes")
                if 'deposit_per_capita' in row:
                    wealth_indicators.append(f"${row['deposit_per_capita']:,.0f} deposits per capita")
                
                f.write(", ".join(wealth_indicators))
                f.write(") but relatively low advisor coverage. ")
                
                if 'gdp_cagr' in row and row['gdp_cagr'] > 0:
                    f.write(f"The market also shows strong economic growth with a {row['gdp_cagr']*100:.1f}% GDP CAGR. ")
                
                f.write("This combination suggests the market may be underserved by wealth management firms.\n\n")
                
                rank += 1
            
            # Methodology
            f.write("\n## Methodology\n\n")
            f.write("Underserved markets were identified using a composite scoring approach:\n\n")
            f.write("1. **Market Potential Score** (60% weight): Combines wealth concentration metrics with economic growth indicators\n")
            f.write("2. **Advisor Coverage** (40% weight): Measures the density of financial advisors relative to population\n\n")
            
            f.write("Markets with high potential scores but low advisor coverage receive the highest underserved scores, ")
            f.write("indicating potential business opportunity. The scoring methodology normalizes all metrics to a 0-100 scale ")
            f.write("to enable fair comparison across MSAs of different sizes.\n\n")
            
            # Conclusion
            f.write("\n## Conclusion\n\n")
            f.write("The top underserved markets identified in this analysis represent significant opportunity for wealth management firms. ")
            f.write("These areas demonstrate high concentrations of wealth and strong economic fundamentals but have less competitive ")
            f.write("advisor coverage compared to similar wealthy markets. Firms looking to expand into new territories should consider ")
            f.write("these markets as prime candidates for growth initiatives.\n\n")
            
            f.write("*Report generated by Wealth Management Market Opportunity Analysis*")
        
        logging.info(f"Created underserved markets report at {report_path}")
        return report_path
    
    except Exception as e:
        logging.error(f"Error creating report: {e}")
        return None

def main():
    """Main function to identify underserved markets and generate report."""
    logging.info("Starting underserved markets analysis...")
    
    # Load data
    data = load_metrics_data()
    if not data:
        logging.error("Failed to load required data. Exiting.")
        return
    
    # Identify underserved markets
    all_markets, underserved = identify_underserved_markets(data, n_markets=15)
    if underserved is None:
        logging.error("Failed to identify underserved markets. Exiting.")
        return
    
    # Create visualizations
    vis_files = create_visualizations(all_markets, underserved)
    
    # Create report
    report_path = create_report(all_markets, underserved, vis_files)
    if report_path:
        logging.info(f"Analysis complete. Report available at: {report_path}")
        
        # Create a simplified CSV with just the top markets
        timestamp = datetime.now().strftime("%Y%m%d")
        csv_path = os.path.join(OUTPUT_DIR, f"top_underserved_markets_{timestamp}.csv")
        underserved.to_csv(csv_path, index=False)
        logging.info(f"Top underserved markets data saved to: {csv_path}")
        
        # Print a message for the user
        print(f"\nAnalysis complete!")
        print(f"Report: {report_path}")
        print(f"Data: {csv_path}")
        for vis in vis_files:
            print(f"Visualization: {vis}")

if __name__ == "__main__":
    main()