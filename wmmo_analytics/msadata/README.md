# MSA-Level Wealth Management Market Opportunity Data

This directory contains Metropolitan Statistical Area (MSA) level data aggregated from various sources for the Wealth Management Market Opportunity analysis.

## Files Generated on 20250416

### Combined Data
- `msa_combined_metrics_20250416.csv`: Combined metrics for wealth management opportunity analysis
- `msa_reference_20250416.csv`: Reference table of all MSAs

### BEA Data (Bureau of Economic Analysis)
- `msa_bea_gdp_20250416.csv`: GDP data by MSA
- `msa_bea_gdp_growth_20250416.csv`: GDP growth rates by MSA
- `msa_bea_population_20250416.csv`: Population data by MSA
- `msa_bea_personal_income_20250416.csv`: Personal income data by MSA
- `msa_bea_per_capita_income_calculated_20250416.csv`: Calculated per capita income by MSA
- `msa_bea_income_by_industry_20250416.csv`: Detailed income by industry data at MSA level

### Census ACS Data (American Community Survey)
- `msa_census_acs_20250416.csv`: Demographics and housing data at MSA level

### FDIC Data (Federal Deposit Insurance Corporation)
- `msa_fdic_YYYY_20250416.csv`: Deposit data by MSA for each year YYYY

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
