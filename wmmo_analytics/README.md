# Wealth Management Market Opportunity Analytics

This directory contains the analytics components for the Wealth Management Market Opportunity (WMMO) analysis project. These scripts implement Phase 1 of the project plan, focusing on data integration, transformation, and metric calculation.

## Directory Structure

- **crosswalks/**: County-to-MSA crosswalk files and scripts
  - `census_cbsa_crosswalk.py`: Script to download and process Census CBSA definitions
  - Generated files: Crosswalk mappings between counties and MSAs

- **msadata/**: MSA-level aggregated data
  - `aggregate_county_to_msa.py`: Script to aggregate county-level data to MSA level
  - Generated files: MSA-level data aggregated from BEA, Census ACS, IRS SOI, and FDIC sources

- **metrics/**: Market opportunity metric calculations
  - `hnwi_density_metrics.py`: Calculates High-Net-Worth Individual density metrics
  - `financial_services_metrics.py`: Calculates financial services coverage metrics
  - `economic_vitality_metrics.py`: Calculates economic vitality indicators
  - Generated files: Calculated metrics, rankings, and opportunity scores

## Main Scripts

- **run_phase1.py**: Main script to execute the complete Phase 1 pipeline
  - Creates county-to-MSA crosswalk
  - Aggregates county data to MSA level
  - Calculates all metrics
  - Generates a summary report of Phase 1 results

## Data Flow

1. **Data Sources** (parent directory)
   - BEA Regional Economic Accounts data (`../bea_data/`)
   - Census ACS data (`../census_acs_data/`)
   - IRS SOI ZIP code data (`../irs_soi_data/`)
   - FDIC deposit data (`../fdic_deposit_data/`)
   - BLS advisor data (`../bls_advisors_data/`)

2. **Data Processing**
   - County-to-MSA crosswalk creation
   - County-level data aggregation to MSA level
   - Metric calculation and normalization

3. **Output**
   - MSA-level data files
   - Calculated metrics with rankings
   - Market opportunity scores and classifications

## Usage

To run the complete Phase 1 pipeline:

```bash
python run_phase1.py
```

### Command Line Options

- `--refresh-data`: Re-download data from original sources
- `--skip-crosswalk`: Skip county-to-MSA crosswalk creation
- `--skip-aggregation`: Skip county-to-MSA data aggregation

### Example

```bash
python run_phase1.py --skip-crosswalk
```

This will run the pipeline, skipping the crosswalk creation step (useful if the crosswalk has already been created).

## Metrics Calculated

The analysis produces three categories of metrics:

1. **HNWI Density Metrics**
   - HNWI Density Index
   - Wealth Growth Rate
   - Luxury Real Estate Quotient
   - Income Elite Ratio
   - Banking Deposit Intensity

2. **Financial Services Coverage Metrics**
   - Advisor Penetration Rate
   - HNWI-to-Advisor Ratio
   - Market Opportunity Score

3. **Economic Vitality Indicators**
   - GDP Growth Trend
   - Wealth-Creating Industries
   - Executive Density
   - Economic Vitality Index

These metrics are combined to create an overall market opportunity ranking for each MSA.

## Next Steps

After completing Phase 1, the project will proceed to:

- **Phase 2**: Fill data gaps with alternative sources and proxy metrics
- **Phase 3**: Develop visualization and opportunity scoring platform