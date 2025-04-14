# Census CBSA-to-County Crosswalk

This directory contains crosswalk files mapping counties to Core-Based Statistical Areas (CBSAs), 
which include Metropolitan and Micropolitan Statistical Areas. These files were downloaded from 
the U.S. Census Bureau on 20250414.

## Files

- `cbsa_tiger_20250414.zip`: TIGER/Line shapefile for CBSAs
- `county_tiger_20250414.zip`: TIGER/Line shapefile for counties
- `cbsa_info_20250414.csv`: Processed file containing information about CBSAs
- `metro_counties_20250414.csv`: Processed file containing county-to-CBSA mappings (metropolitan areas only)
- `cbsa_mappings_20250414.json`: JSON file with useful mappings (CBSA to title, county to CBSA, etc.)
- `cbsa_crosswalk_data_20250414.pkl`: Python pickle file containing all crosswalk data

## Data Sources

- CBSA TIGER/Line Shapefile: https://www2.census.gov/geo/tiger/TIGER2023/CBSA/tl_2023_us_cbsa.zip
- County TIGER/Line Shapefile: https://www2.census.gov/geo/tiger/TIGER2023/COUNTY/tl_2023_us_county.zip

These files are based on the 2023 CBSA delineations by the Office of Management and Budget.

## Usage

The pickle file contains a dictionary with the following keys:

- `cbsa_info`: DataFrame with information about CBSAs
- `metro_counties`: DataFrame with county-to-CBSA mappings
- `mappings`: Dictionary with useful mappings:
  - `cbsa_to_title`: CBSA code to CBSA title
  - `cbsa_counties`: CBSA code to list of county FIPS codes
  - `county_to_cbsa`: County FIPS code to CBSA code
  - `states_in_cbsa`: CBSA code to list of state FIPS codes

To load the pickle file:

```python
import pickle
with open('cbsa_crosswalk_data_20250414.pkl', 'rb') as f:
    crosswalk_data = pickle.load(f)
```

## Important Notes

This script uses TIGER/Line shapefiles instead of the direct delineation files
since the Census Bureau has updated their file structure. For a complete implementation,
you'll need to either:

1. Register for a Census API key and implement the get_cbsa_relationships_from_api() function
2. Manually download the delineation files and parse them
3. Use spatial analysis to determine county-to-CBSA relationships

