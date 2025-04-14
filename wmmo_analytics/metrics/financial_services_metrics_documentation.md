# Financial Services Coverage Metrics

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

## Files Generated on 20250414

- `financial_services_metrics_20250414.csv`: Complete metrics for all MSAs
- `coverage_opportunity_rankings_20250414.csv`: Simplified rankings with key metrics
- `top_market_opportunities_20250414.csv`: Detailed profile of top market opportunities

## Notes

- Data is derived from BLS advisor statistics and Census wealth indicators
- The "Wealth Management Saturation" metric requires additional data on market share of top firms
- The "Average AUM per Advisor" metric requires SEC data not currently available
- The "Service Mix Alignment" metric requires data on advisor specializations not currently available
