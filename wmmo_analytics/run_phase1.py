#!/usr/bin/env python3
"""
run_phase1.py

This script runs the complete Phase 1 implementation of the Wealth Management
Market Opportunity Analysis, including:

1. County to MSA Aggregation
   - Create county-to-MSA crosswalk using Census CBSA definitions
   - Aggregate county-level data (BEA, Census, IRS) to MSA level
   - Validate MSA-level aggregations against known MSA statistics

2. Metric Calculation for Available Indicators
   - Implement calculations for all "AVAILABLE" metrics
   - Develop estimation methodologies for "PARTIAL" metrics
   - Document assumptions and limitations in all calculations

3. Data Normalization and Time Series Creation
   - Normalize all metrics for comparability (per capita, percentages, etc.)
   - Create time series views where data permits (2018-2022/2023)
   - Calculate growth rates and trends for key indicators

Usage: python run_phase1.py [--refresh-data] [--skip-crosswalk] [--skip-aggregation]
"""

import os
import sys
import argparse
import subprocess
import logging
import glob
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("wmmo_phase1.log"),
        logging.StreamHandler()
    ]
)

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Run Phase 1 of the Wealth Management Market Opportunity Analysis")
    parser.add_argument("--refresh-data", action="store_true", help="Re-download data from original sources")
    parser.add_argument("--skip-crosswalk", action="store_true", help="Skip county-to-MSA crosswalk creation")
    parser.add_argument("--skip-aggregation", action="store_true", help="Skip county-to-MSA data aggregation")
    return parser.parse_args()

def run_script(script_path, description):
    """
    Run a Python script and log its output.
    
    Args:
        script_path (str): Path to the script to run
        description (str): Description of the script for logging
    
    Returns:
        bool: True if script executed successfully, False otherwise
    """
    logging.info(f"Starting: {description}")
    try:
        # Run the script as a subprocess
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            check=False  # Don't raise exception on non-zero exit
        )
        
        # Log stdout
        for line in result.stdout.splitlines():
            logging.info(f"  {line}")
        
        # Log stderr if there was any
        if result.stderr:
            for line in result.stderr.splitlines():
                logging.warning(f"  {line}")
        
        # Check return code
        if result.returncode != 0:
            logging.error(f"Failed: {description} (Return code: {result.returncode})")
            return False
        
        logging.info(f"Completed: {description}")
        return True
    
    except Exception as e:
        logging.error(f"Error running {script_path}: {e}")
        return False

def refresh_data():
    """
    Re-download data from original sources.
    
    Returns:
        bool: True if all data refreshes were successful, False otherwise
    """
    logging.info("Refreshing data from original sources...")
    
    # Define the data collection scripts
    data_scripts = [
        {"path": "../data_bea.py", "desc": "BEA Regional Economic Accounts data collection"},
        {"path": "../data_census_acs.py", "desc": "Census ACS data collection"},
        {"path": "../data_irs_soi.py", "desc": "IRS SOI data collection"},
        {"path": "../data_fdic_deposits.py", "desc": "FDIC deposit data collection"},
        {"path": "../data_bls_advisors.py", "desc": "BLS advisors data collection"}
    ]
    
    success = True
    for script in data_scripts:
        script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), script["path"]))
        if os.path.exists(script_path):
            if not run_script(script_path, script["desc"]):
                success = False
        else:
            logging.warning(f"Script not found: {script_path}")
            success = False
    
    return success

def check_output_files(directory, pattern, min_count=1):
    """
    Check if a minimum number of output files exist.
    
    Args:
        directory (str): Directory to check
        pattern (str): Glob pattern to match files
        min_count (int): Minimum number of files expected
    
    Returns:
        bool: True if minimum number of files found, False otherwise
    """
    files = glob.glob(os.path.join(directory, pattern))
    if len(files) >= min_count:
        logging.info(f"Found {len(files)} files matching {pattern} in {directory}")
        return True
    else:
        logging.warning(f"Expected at least {min_count} files matching {pattern} in {directory}, but found {len(files)}")
        return False

def create_summary_report():
    """
    Create a summary report of all files generated during Phase 1.
    
    Returns:
        str: Path to the generated report
    """
    logging.info("Creating Phase 1 summary report...")
    
    timestamp = datetime.now().strftime("%Y%m%d")
    report_path = os.path.join(os.path.dirname(__file__), f"phase1_summary_{timestamp}.md")
    
    # Define directories to scan for output files
    directories = [
        {"path": "crosswalks", "desc": "County-to-MSA Crosswalk Files"},
        {"path": "msadata", "desc": "MSA-Level Aggregated Data"},
        {"path": "metrics", "desc": "Calculated Market Opportunity Metrics"}
    ]
    
    try:
        with open(report_path, 'w') as f:
            f.write("# Wealth Management Market Opportunity Analysis - Phase 1 Summary\n\n")
            f.write(f"*Generated on {datetime.now().strftime('%Y-%m-%d')}*\n\n")
            
            f.write("## Overview\n\n")
            f.write("Phase 1 of the Wealth Management Market Opportunity Analysis includes:\n\n")
            f.write("1. County to MSA Aggregation\n")
            f.write("2. Metric Calculation for Available Indicators\n")
            f.write("3. Data Normalization and Time Series Creation\n\n")
            
            # List files in each directory
            for directory in directories:
                dir_path = os.path.join(os.path.dirname(__file__), directory["path"])
                if os.path.exists(dir_path):
                    f.write(f"## {directory['desc']}\n\n")
                    
                    # Look for CSV files
                    csv_files = glob.glob(os.path.join(dir_path, "*.csv"))
                    if csv_files:
                        f.write("### Data Files\n\n")
                        for file_path in sorted(csv_files):
                            file_name = os.path.basename(file_path)
                            file_size = os.path.getsize(file_path) / 1024  # Size in KB
                            f.write(f"- `{file_name}` ({file_size:.1f} KB)\n")
                        f.write("\n")
                    
                    # Look for documentation files
                    doc_files = glob.glob(os.path.join(dir_path, "*.md"))
                    if doc_files:
                        f.write("### Documentation\n\n")
                        for file_path in sorted(doc_files):
                            file_name = os.path.basename(file_path)
                            if file_name != f"phase1_summary_{timestamp}.md":  # Skip this report
                                f.write(f"- `{file_name}`\n")
                        f.write("\n")
            
            # Add section for key metrics
            f.write("## Key Metrics Generated\n\n")
            
            # HNWI Density Metrics
            f.write("### HNWI Density Metrics\n\n")
            f.write("| Metric | Status | Source | Notes |\n")
            f.write("|--------|--------|--------|-------|\n")
            
            hnwi_file = glob.glob(os.path.join(os.path.dirname(__file__), "metrics", "hnwi_density_metrics_*.csv"))
            if hnwi_file:
                f.write("| HNWI Density Index | **AVAILABLE** | Calculated | Composite metric of wealth indicators |\n")
                f.write("| Wealth Growth Rate | **AVAILABLE** | BEA | Calculated as GDP CAGR |\n")
                f.write("| Luxury Real Estate Quotient | **AVAILABLE** | Census ACS | Based on homes valued >$1M |\n")
                f.write("| Income Elite Ratio | **AVAILABLE** | Census ACS | Based on households with income $200K+ |\n")
                f.write("| Banking Deposit Intensity | **AVAILABLE** | FDIC | Total deposits per capita |\n")
            else:
                f.write("| HNWI Density Metrics | **NOT CALCULATED** | - | HNWI metrics calculation script needs to be run |\n")
            
            # Financial Services Coverage Metrics
            f.write("\n### Financial Services Coverage Metrics\n\n")
            f.write("| Metric | Status | Source | Notes |\n")
            f.write("|--------|--------|--------|-------|\n")
            
            fs_file = glob.glob(os.path.join(os.path.dirname(__file__), "metrics", "financial_services_metrics_*.csv"))
            if fs_file:
                f.write("| Advisor Penetration Rate | **AVAILABLE** | BLS | Registered advisors per 10,000 residents |\n")
                f.write("| HNWI-to-Advisor Ratio | **AVAILABLE** | Calculated | Estimated high-income households per advisor |\n")
                f.write("| Wealth Management Saturation | **GAP** | - | Requires additional data on market share |\n")
                f.write("| Average AUM per Advisor | **GAP** | - | Requires SEC data |\n")
                f.write("| Service Mix Alignment | **GAP** | - | Requires data on advisor specializations |\n")
            else:
                f.write("| Financial Services Metrics | **NOT CALCULATED** | - | Financial services metrics calculation script needs to be run |\n")
            
            # Economic Vitality Indicators
            f.write("\n### Economic Vitality Indicators\n\n")
            f.write("| Metric | Status | Source | Notes |\n")
            f.write("|--------|--------|--------|-------|\n")
            
            econ_file = glob.glob(os.path.join(os.path.dirname(__file__), "metrics", "economic_vitality_metrics_*.csv"))
            if econ_file:
                f.write("| GDP Growth Trend | **AVAILABLE** | BEA | 5-year CAGR of MSA GDP |\n")
                f.write("| Wealth-Creating Industries | **AVAILABLE** | BEA | Industry composition analysis |\n")
                f.write("| Business Formation Rate | **GAP** | - | Requires data on new businesses |\n")
                f.write("| Executive Density | **PARTIAL** | Census | Estimated from income data |\n")
                f.write("| Entrepreneur Quotient | **GAP** | - | Requires data on business exits and liquidity events |\n")
            else:
                f.write("| Economic Vitality Metrics | **NOT CALCULATED** | - | Economic vitality metrics calculation script needs to be run |\n")
            
            # Overall Market Opportunity
            f.write("\n### Overall Market Opportunity\n\n")
            
            overall_file = glob.glob(os.path.join(os.path.dirname(__file__), "metrics", "overall_market_opportunity_*.csv"))
            if overall_file:
                f.write("A comprehensive market opportunity score and ranking has been calculated, combining all three dimensions:\n\n")
                f.write("1. Wealth concentration (from HNWI Density metrics)\n")
                f.write("2. Market opportunity (from Financial Services Coverage metrics)\n")
                f.write("3. Economic foundation (from Economic Vitality metrics)\n")
            else:
                f.write("The overall market opportunity has not yet been calculated. Run the economic vitality metrics script to generate this.\n")
            
            # Next Steps
            f.write("\n## Next Steps\n\n")
            f.write("1. **Phase 2: Gap Filling Strategies**\n")
            f.write("   - Investigate additional data sources for missing metrics\n")
            f.write("   - Develop proxy metrics for gaps\n\n")
            
            f.write("2. **Phase 3: Analysis & Visualization Platform**\n")
            f.write("   - Develop interactive dashboard for exploring results\n")
            f.write("   - Create weighted opportunity scoring model\n")
            f.write("   - Build filtering capabilities by region and market characteristics\n")
        
        logging.info(f"Created Phase 1 summary report at {report_path}")
        return report_path
    
    except Exception as e:
        logging.error(f"Error creating summary report: {e}")
        return None

def main():
    """Main function to run Phase 1 implementation"""
    logging.info("Starting Phase 1 of Wealth Management Market Opportunity Analysis")
    
    # Parse command line arguments
    args = parse_args()
    
    # Step 0: Refresh data if requested
    if args.refresh_data:
        if not refresh_data():
            logging.warning("Data refresh had issues, but continuing with existing data")
    
    # Step 1: Create county-to-MSA crosswalk
    if not args.skip_crosswalk:
        crosswalk_script = os.path.join(os.path.dirname(__file__), "crosswalks", "census_cbsa_crosswalk.py")
        if not run_script(crosswalk_script, "County-to-MSA crosswalk creation"):
            logging.error("Failed to create crosswalk. Exiting.")
            return
        
        # Verify crosswalk files were created
        if not check_output_files(os.path.join(os.path.dirname(__file__), "crosswalks"), "cbsa_crosswalk_data_*.pkl"):
            logging.error("Crosswalk files not found. Exiting.")
            return
    else:
        logging.info("Skipping crosswalk creation as requested")
    
    # Step 2: Aggregate county-level data to MSA level
    if not args.skip_aggregation:
        aggregation_script = os.path.join(os.path.dirname(__file__), "msadata", "aggregate_county_to_msa.py")
        if not run_script(aggregation_script, "County-to-MSA data aggregation"):
            logging.error("Failed to aggregate data. Exiting.")
            return
        
        # Verify MSA data files were created
        if not check_output_files(os.path.join(os.path.dirname(__file__), "msadata"), "msa_combined_metrics_*.csv"):
            logging.error("MSA data files not found. Exiting.")
            return
    else:
        logging.info("Skipping data aggregation as requested")
    
    # Step 3: Calculate HNWI density metrics
    hnwi_script = os.path.join(os.path.dirname(__file__), "metrics", "hnwi_density_metrics.py")
    if not run_script(hnwi_script, "HNWI density metrics calculation"):
        logging.warning("Failed to calculate HNWI density metrics. Continuing with other metrics.")
    
    # Step 4: Calculate financial services coverage metrics
    fs_script = os.path.join(os.path.dirname(__file__), "metrics", "financial_services_metrics.py")
    if not run_script(fs_script, "Financial services coverage metrics calculation"):
        logging.warning("Failed to calculate financial services metrics. Continuing with other metrics.")
    
    # Step 5: Calculate economic vitality metrics
    econ_script = os.path.join(os.path.dirname(__file__), "metrics", "economic_vitality_metrics.py")
    if not run_script(econ_script, "Economic vitality metrics calculation"):
        logging.warning("Failed to calculate economic vitality metrics.")
    
    # Step 6: Create summary report
    summary_report = create_summary_report()
    if summary_report:
        logging.info(f"Phase 1 summary report available at: {summary_report}")
    
    logging.info("Phase 1 of Wealth Management Market Opportunity Analysis complete!")

if __name__ == "__main__":
    main()