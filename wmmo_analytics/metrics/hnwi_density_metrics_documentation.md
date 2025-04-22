# HNWI Density Metrics

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

## Files Generated on 20250416

- `hnwi_density_metrics_20250416.csv`: Complete metrics for all MSAs
- `hnwi_density_rankings_20250416.csv`: Simplified rankings with key metrics

## Notes

- Data is aggregated from various sources including BEA, Census ACS, and FDIC
- Missing values indicate the data was not available for that MSA
- Rankings should be interpreted with caution when data is incomplete
