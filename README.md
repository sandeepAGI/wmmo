# Wealth Management Market Opportunity Analysis

This project analyzes public data sources to identify underserved markets for wealth management services. It aggregates economic, demographic, and financial data at the Metropolitan Statistical Area (MSA) level to calculate opportunity metrics and rankings.

## Project Organization

1. **Data Collection**: Scripts to collect data from publicly available sources
2. **Data Analysis**: Scripts to aggregate county-level data to MSA level and calculate metrics
3. **Metrics & Rankings**: Calculation of wealth management opportunity metrics and market rankings

## API Keys Setup

This project requires API keys from various data providers. To set up the required keys:

1. Copy `secrets_template.py` to `secrets.py`:
   ```bash
   cp secrets_template.py secrets.py
   ```

2. Edit `secrets.py` and add your actual API keys:
   - BEA_API_KEY: Get from [BEA API](https://apps.bea.gov/API/signup/)
   - CENSUS_API_KEY: Get from [Census API](https://api.census.gov/data/key_signup.html)

The `secrets.py` file is in `.gitignore` to ensure API keys are never committed to the repository.

## Data Sources

1. `data_census_acs.py` - Census American Community Survey (ACS) data at county level
   - Demographics, income distribution, housing values, education levels
   - Data and dictionary in `census_acs_data` folder

2. `data_bea.py` - Bureau of Economic Analysis (BEA) Regional Economic Accounts data
   - GDP, personal income, industry composition at county level
   - Data and dictionary in `bea_data` folder

3. `data_irs_soi.py` - IRS Statistics of Income (SOI) ZIP Code data
   - Income, wealth indicators from tax returns
   - Data collected for 2018-2022 in `irs_soi_data` folder

4. `data_bls_advisors.py` - Bureau of Labor Statistics (BLS) data on financial advisors
   - MSA-level information on financial advisors
   - Used Selenium with chromedriver for collection
   - Data in `bls_advisors_data` folder

5. `data_fdic_deposits.py` - FDIC Summary of Deposits data
   - Branch-level deposit information
   - Data in `fdic_deposit_data` folder

**Note**: The original plan included SEC and FINRA data for advisor counts, but due to API rate limits, the project now uses BLS data instead.

## Analytics Components (Phase 1 Implementation)

The analytics components have been implemented in the `wmmo_analytics` directory:

1. **County-to-MSA Aggregation**
   - County-to-MSA crosswalk using Census CBSA definitions
   - Aggregation of county-level data to MSA level
   - Data validation and quality checks

2. **Metric Calculation**
   - HNWI Density metrics (wealth concentration)
   - Financial Services Coverage metrics (advisor coverage)
   - Economic Vitality indicators (economic fundamentals)

3. **Market Opportunity Analysis**
   - Composite rankings and opportunity scores
   - Identification of underserved markets
   - Detailed market profiles

See `wmmo_analytics/README.md` for details on the analytics implementation.

## Running the Analysis

The complete Phase 1 analysis can be run with:

```bash
cd wmmo_analytics
python run_phase1.py
```

This will create all necessary crosswalks, aggregate data to MSA level, calculate all metrics, and generate a summary report of the results.

## Project Status

- ✅ Phase 1: Data Integration and Transformation (complete)
- ⬜ Phase 2: Gap Filling Strategies
- ⬜ Phase 3: Analysis & Visualization Platform

See `nextsteps.md` for the detailed implementation plan.

## Data Directories

All data files are in subfolders that end with `..._data`, which also contain data dictionaries:

- `bea_data/`: BEA Regional Economic Accounts data
- `census_acs_data/`: Census ACS data
- `irs_soi_data/`: IRS SOI data
- `fdic_deposit_data/`: FDIC deposit data
- `bls_advisors_data/`: BLS advisor data
- `wmmo_analytics/`: Analysis scripts and output files