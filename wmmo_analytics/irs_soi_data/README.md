# IRS Statistics of Income (SOI) ZIP Code Data

This directory contains IRS SOI ZIP Code data collected on 2025-04-14.

## Data Overview
- Data source: IRS Statistics of Income (SOI) Individual Income Tax Statistics
- Geographic level: ZIP Code
- Years available: 2022, 2021, 2020, 2019, 2018

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
