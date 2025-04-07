# BEA Regional Economic Accounts Data Dictionary

*Generated on 2025-04-06*

## Dataset Description

Bureau of Economic Analysis (BEA) Regional Economic Accounts data for counties. This dataset includes economic indicators such as GDP, personal income, employment by industry, and other metrics for comparing economic conditions across counties. The data can be aggregated to MSA level for metropolitan analysis.

## Tables

- **CAGDP9**: Real GDP by county and metropolitan area
- **CAGDP2**: Gross domestic product (GDP) by county and metropolitan area by industry
- **CAINC1**: County and MSA personal income summary: personal income, population, per capita personal income
- **CAINC5N**: Personal income by major component and earnings by NAICS industry
- **CAINC6N**: Compensation of employees by NAICS industry
- **CAINC30**: Economic profile

## Common Fields

These fields appear in most or all data tables:

- **GeoFips**: Geographic FIPS code for the county
- **GeoName**: Geographic name of the county
- **TimePeriod**: Time period for the data point (e.g., 2020 for year 2020)
- **CL_UNIT**: Classification of the unit of measure
- **UNIT_MULT**: The multiplier for the data value
- **DataValue**: The actual data value
- **NoteRef**: Reference to any notes applicable to the data point

## Table-Specific Fields

### CAGDP9 (Real GDP by county and metropolitan area)

- **LineCode**: Line code for different types of GDP measures (1 is All industries total)
- **IndustryClassification**: Classification system for industries (typically NAICS)

### CAGDP2 (Gross domestic product (GDP) by county and metropolitan area by industry)

- **LineCode**: Line code for different industry categories
- **IndustryClassification**: Classification system for industries (typically NAICS)

### CAINC1 (County and MSA personal income summary: personal income, population, per capita personal income)

- **LineCode**: Line code for different income categories (1: Personal income, 2: Population, 3: Per capita personal income)

### CAINC5N (Personal income by major component and earnings by NAICS industry)

- **LineCode**: Line code for different industry categories and components
- **IndustryClassification**: Classification system for industries (NAICS)

### CAINC6N (Compensation of employees by NAICS industry)

- **LineCode**: Line code for different industry categories
- **IndustryClassification**: Classification system for industries (NAICS)

### CAINC30 (Economic profile)

- **LineCode**: Line code for different economic indicators
- **Description**: Description of the economic indicator

## Line Code Descriptions

### CAINC1 (County and MSA personal income summary: personal income, population, per capita personal income)

- **1**: Personal income (thousands of dollars)
- **2**: Population (persons)
- **3**: Per capita personal income (dollars)

## Frequency Codes

- **A**: Annual data
