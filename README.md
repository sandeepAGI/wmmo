# Wealth Management Opportunity Framework

1. Collect data from publlicly available sources (all in format data_<source>.py)
2. Analyze them to identify underserved markets
3. The SEC Data has a free API but with only 100 requests that I met.  So the alternative was to stop using the SEC and FINRA data to find number of advisors, as originally planned but to rely instead on BLS data
4. All the data files are in subfolders that end with '..._data' that also has data dictionaries 

## Data Sources

1. data_census_acs.py - county level data (had issues with MSA and will need to use the census CBSA crosswalk file).  Data and dictionary in census_acs_data folder
2. data_bea.py - Bureau of Economic Analysis (BEA) Regional Economic Accounts data for counties.  Data and dictionary in bea_data folder
3. data_irs_soi.py - IRS SOI ZIP Code data collected for 2018 to 2022 - multiple xlxs files
4. data_bls_advisors.py - MSA level information on advisors NOTE: Used Selenium with chromedriver
5. data_fdic_deposits.py - Branch level deposit information 