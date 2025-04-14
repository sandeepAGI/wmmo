import os
import requests
import pandas as pd
import json
from datetime import datetime
import time

class BEADataCollector:
    """
    A class to collect regional economic data from the Bureau of Economic Analysis (BEA) API
    focusing on county-level data.
    """
    
    def __init__(self, api_key):
        """
        Initialize the BEA data collector with API key and base URL.
        
        Args:
            api_key (str): Your BEA API key
        """
        self.api_key = api_key
        self.base_url = "https://apps.bea.gov/api/data"
        self.output_dir = "bea_data"
        
        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            print(f"Created directory: {self.output_dir}")
    
    def get_dataset_list(self):
        """Get list of available datasets from BEA API"""
        params = {
            "UserID": self.api_key,
            "method": "GetDatasetList",
            "ResultFormat": "JSON"
        }
        
        response = requests.get(self.base_url, params=params)
        if response.status_code == 200:
            data = response.json()
            filename = f"{self.output_dir}/available_datasets.json"
            with open(filename, 'w') as f:
                json.dump(data, f, indent=4)
            print(f"Saved available datasets to {filename}")
            return data
        else:
            print(f"Error: {response.status_code}")
            return None
    
    def get_parameters_for_dataset(self, dataset_name):
        """
        Get parameters for a specific dataset
        
        Args:
            dataset_name (str): Name of the dataset (e.g., 'Regional')
        """
        params = {
            "UserID": self.api_key,
            "method": "GetParameterList",
            "datasetname": dataset_name,
            "ResultFormat": "JSON"
        }
        
        response = requests.get(self.base_url, params=params)
        if response.status_code == 200:
            data = response.json()
            filename = f"{self.output_dir}/parameters_{dataset_name}.json"
            with open(filename, 'w') as f:
                json.dump(data, f, indent=4)
            print(f"Saved parameters for {dataset_name} to {filename}")
            return data
        else:
            print(f"Error: {response.status_code}")
            return None
    
    def get_parameter_values(self, dataset_name, parameter_name):
        """
        Get possible values for a specific parameter within a dataset
        
        Args:
            dataset_name (str): Name of the dataset (e.g., 'Regional')
            parameter_name (str): Name of the parameter to get values for
        """
        params = {
            "UserID": self.api_key,
            "method": "GetParameterValues",
            "datasetname": dataset_name,
            "parametername": parameter_name,
            "ResultFormat": "JSON"
        }
        
        response = requests.get(self.base_url, params=params)
        if response.status_code == 200:
            data = response.json()
            filename = f"{self.output_dir}/{dataset_name}_{parameter_name}_values.json"
            with open(filename, 'w') as f:
                json.dump(data, f, indent=4)
            print(f"Saved {parameter_name} values for {dataset_name} to {filename}")
            return data
        else:
            print(f"Error: {response.status_code}")
            return None
    
    def get_county_gdp_data(self, start_year, end_year, frequency="A"):
        """
        Get GDP data for all counties for the specified timeframe
        
        Args:
            start_year (int): Starting year of data collection
            end_year (int): Ending year of data collection
            frequency (str): Frequency of data ('A' for annual)
        """
        years = ",".join([str(year) for year in range(start_year, end_year + 1)])
        
        params = {
            "UserID": self.api_key,
            "method": "GetData",
            "datasetname": "Regional",
            "TableName": "CAGDP9",  # Real GDP by county and metropolitan area
            "GeoFips": "COUNTY",  # All counties
            "LineCode": 1,  # All industries total (line code 1)
            "Frequency": frequency,
            "Year": years,
            "ResultFormat": "JSON"
        }
        
        print(f"Requesting County GDP data for years {start_year}-{end_year}...")
        response = requests.get(self.base_url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            # Save raw response for debugging
            raw_file = f"{self.output_dir}/raw_county_gdp_{start_year}_{end_year}_{frequency}.json"
            with open(raw_file, 'w') as f:
                json.dump(data, f, indent=4)
            print(f"Saved raw GDP response to {raw_file}")
            
            # Check for error messages in the response
            if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
                if 'Error' in data['BEAAPI']['Results']:
                    error = data['BEAAPI']['Results']['Error']
                    print(f"API Error: {error}")
                    return None
                
                if 'Data' in data['BEAAPI']['Results']:
                    df = pd.DataFrame(data['BEAAPI']['Results']['Data'])
                    filename = f"{self.output_dir}/county_gdp_{start_year}_{end_year}_{frequency}.csv"
                    df.to_csv(filename, index=False)
                    print(f"Saved GDP data to {filename}")
                    return df
                else:
                    print("No data found in API response")
                    return pd.DataFrame()
            else:
                print("Unexpected API response structure")
                return pd.DataFrame()
        else:
            print(f"Error {response.status_code}: {response.text}")
            return None
    
    def get_county_gdp_by_industry(self, start_year, end_year, frequency="A"):
        """
        Get GDP data by industry for all counties for the specified timeframe
        
        Args:
            start_year (int): Starting year of data collection
            end_year (int): Ending year of data collection
            frequency (str): Frequency of data ('A' for annual)
        """
        years = ",".join([str(year) for year in range(start_year, end_year + 1)])
        
        params = {
            "UserID": self.api_key,
            "method": "GetData",
            "datasetname": "Regional",
            "TableName": "CAGDP2",  # GDP by county and metropolitan area by industry
            "GeoFips": "COUNTY",  # All counties
            "Frequency": frequency,
            "Year": years,
            "ResultFormat": "JSON"
        }
        
        print(f"Requesting County GDP by Industry data for years {start_year}-{end_year}...")
        response = requests.get(self.base_url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            # Save raw response for debugging
            raw_file = f"{self.output_dir}/raw_county_gdp_by_industry_{start_year}_{end_year}_{frequency}.json"
            with open(raw_file, 'w') as f:
                json.dump(data, f, indent=4)
            print(f"Saved raw GDP by Industry response to {raw_file}")
            
            # Check for error messages in the response
            if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
                if 'Error' in data['BEAAPI']['Results']:
                    error = data['BEAAPI']['Results']['Error']
                    print(f"API Error: {error}")
                    return None
                
                if 'Data' in data['BEAAPI']['Results']:
                    df = pd.DataFrame(data['BEAAPI']['Results']['Data'])
                    filename = f"{self.output_dir}/county_gdp_by_industry_{start_year}_{end_year}_{frequency}.csv"
                    df.to_csv(filename, index=False)
                    print(f"Saved GDP by Industry data to {filename}")
                    return df
                else:
                    print("No data found in API response")
                    return pd.DataFrame()
            else:
                print("Unexpected API response structure")
                return pd.DataFrame()
        else:
            print(f"Error {response.status_code}: {response.text}")
            return None
    
    def get_county_personal_income(self, start_year, end_year, frequency="A"):
        """
        Get personal income data for all counties for the specified timeframe
        
        Args:
            start_year (int): Starting year of data collection
            end_year (int): Ending year of data collection
            frequency (str): Frequency of data ('A' for annual)
        """
        years = ",".join([str(year) for year in range(start_year, end_year + 1)])
        
        params = {
            "UserID": self.api_key,
            "method": "GetData",
            "datasetname": "Regional",
            "TableName": "CAINC1",  # Personal Income Summary
            "LineCode": 1,  # Personal income (thousands of dollars)
            "GeoFips": "COUNTY",  # All counties
            "Frequency": frequency,
            "Year": years,
            "ResultFormat": "JSON"
        }
        
        print(f"Requesting County Personal Income data for years {start_year}-{end_year}...")
        response = requests.get(self.base_url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            # Save raw response for debugging
            raw_file = f"{self.output_dir}/raw_county_personal_income_{start_year}_{end_year}_{frequency}.json"
            with open(raw_file, 'w') as f:
                json.dump(data, f, indent=4)
            print(f"Saved raw Personal Income response to {raw_file}")
            
            # Check for error messages in the response
            if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
                if 'Error' in data['BEAAPI']['Results']:
                    error = data['BEAAPI']['Results']['Error']
                    print(f"API Error: {error}")
                    return None
                
                if 'Data' in data['BEAAPI']['Results']:
                    df = pd.DataFrame(data['BEAAPI']['Results']['Data'])
                    filename = f"{self.output_dir}/county_personal_income_{start_year}_{end_year}_{frequency}.csv"
                    df.to_csv(filename, index=False)
                    print(f"Saved Personal Income data to {filename}")
                    return df
                else:
                    print("No data found in API response")
                    return pd.DataFrame()
            else:
                print("Unexpected API response structure")
                return pd.DataFrame()
        else:
            print(f"Error {response.status_code}: {response.text}")
            return None
    
    def get_county_population(self, start_year, end_year, frequency="A"):
        """
        Get population data for all counties for the specified timeframe
        
        Args:
            start_year (int): Starting year of data collection
            end_year (int): Ending year of data collection
            frequency (str): Frequency of data ('A' for annual)
        """
        years = ",".join([str(year) for year in range(start_year, end_year + 1)])
        
        params = {
            "UserID": self.api_key,
            "method": "GetData",
            "datasetname": "Regional",
            "TableName": "CAINC1",  # Personal Income Summary (includes population)
            "LineCode": 2,  # Line code 2 is for population
            "GeoFips": "COUNTY",  # All counties
            "Frequency": frequency,
            "Year": years,
            "ResultFormat": "JSON"
        }
        
        print(f"Requesting County Population data for years {start_year}-{end_year}...")
        response = requests.get(self.base_url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            # Save raw response for debugging
            raw_file = f"{self.output_dir}/raw_county_population_{start_year}_{end_year}_{frequency}.json"
            with open(raw_file, 'w') as f:
                json.dump(data, f, indent=4)
            print(f"Saved raw Population response to {raw_file}")
            
            # Check for error messages in the response
            if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
                if 'Error' in data['BEAAPI']['Results']:
                    error = data['BEAAPI']['Results']['Error']
                    print(f"API Error: {error}")
                    return None
                
                if 'Data' in data['BEAAPI']['Results']:
                    df = pd.DataFrame(data['BEAAPI']['Results']['Data'])
                    filename = f"{self.output_dir}/county_population_{start_year}_{end_year}_{frequency}.csv"
                    df.to_csv(filename, index=False)
                    print(f"Saved Population data to {filename}")
                    return df
                else:
                    print("No data found in API response")
                    return pd.DataFrame()
            else:
                print("Unexpected API response structure")
                return pd.DataFrame()
        else:
            print(f"Error {response.status_code}: {response.text}")
            return None
    
    def get_county_per_capita_income(self, start_year, end_year, frequency="A"):
        """
        Get per capita personal income data for all counties for the specified timeframe
        
        Args:
            start_year (int): Starting year of data collection
            end_year (int): Ending year of data collection
            frequency (str): Frequency of data ('A' for annual)
        """
        years = ",".join([str(year) for year in range(start_year, end_year + 1)])
        
        params = {
            "UserID": self.api_key,
            "method": "GetData",
            "datasetname": "Regional",
            "TableName": "CAINC1",  # Personal Income Summary (includes per capita)
            "LineCode": 3,  # Line code 3 is for per capita personal income
            "GeoFips": "COUNTY",  # All counties
            "Frequency": frequency,
            "Year": years,
            "ResultFormat": "JSON"
        }
        
        print(f"Requesting County Per Capita Income data for years {start_year}-{end_year}...")
        response = requests.get(self.base_url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            # Save raw response for debugging
            raw_file = f"{self.output_dir}/raw_county_per_capita_income_{start_year}_{end_year}_{frequency}.json"
            with open(raw_file, 'w') as f:
                json.dump(data, f, indent=4)
            print(f"Saved raw Per Capita Income response to {raw_file}")
            
            # Check for error messages in the response
            if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
                if 'Error' in data['BEAAPI']['Results']:
                    error = data['BEAAPI']['Results']['Error']
                    print(f"API Error: {error}")
                    return None
                
                if 'Data' in data['BEAAPI']['Results']:
                    df = pd.DataFrame(data['BEAAPI']['Results']['Data'])
                    filename = f"{self.output_dir}/county_per_capita_income_{start_year}_{end_year}_{frequency}.csv"
                    df.to_csv(filename, index=False)
                    print(f"Saved Per Capita Income data to {filename}")
                    return df
                else:
                    print("No data found in API response")
                    return pd.DataFrame()
            else:
                print("Unexpected API response structure")
                return pd.DataFrame()
        else:
            print(f"Error {response.status_code}: {response.text}")
            return None
    
    def get_county_income_by_industry(self, start_year, end_year, frequency="A"):
        """
        Get income by industry data for all counties for the specified timeframe
        
        Args:
            start_year (int): Starting year of data collection
            end_year (int): Ending year of data collection
            frequency (str): Frequency of data ('A' for annual)
        """
        years = ",".join([str(year) for year in range(start_year, end_year + 1)])
        
        params = {
            "UserID": self.api_key,
            "method": "GetData",
            "datasetname": "Regional",
            "TableName": "CAINC5N",  # Personal income by major component and earnings by NAICS industry
            "GeoFips": "COUNTY",  # All counties
            "Frequency": frequency,
            "Year": years,
            "ResultFormat": "JSON"
        }
        
        print(f"Requesting County Income by Industry data for years {start_year}-{end_year}...")
        response = requests.get(self.base_url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            # Save raw response for debugging
            raw_file = f"{self.output_dir}/raw_county_income_by_industry_{start_year}_{end_year}_{frequency}.json"
            with open(raw_file, 'w') as f:
                json.dump(data, f, indent=4)
            print(f"Saved raw Income by Industry response to {raw_file}")
            
            # Check for error messages in the response
            if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
                if 'Error' in data['BEAAPI']['Results']:
                    error = data['BEAAPI']['Results']['Error']
                    print(f"API Error: {error}")
                    return None
                
                if 'Data' in data['BEAAPI']['Results']:
                    df = pd.DataFrame(data['BEAAPI']['Results']['Data'])
                    filename = f"{self.output_dir}/county_income_by_industry_{start_year}_{end_year}_{frequency}.csv"
                    df.to_csv(filename, index=False)
                    print(f"Saved Income by Industry data to {filename}")
                    return df
                else:
                    print("No data found in API response")
                    return pd.DataFrame()
            else:
                print("Unexpected API response structure")
                return pd.DataFrame()
        else:
            print(f"Error {response.status_code}: {response.text}")
            return None
    
    def get_county_compensation_by_industry(self, start_year, end_year, frequency="A"):
        """
        Get compensation by industry data for all counties for the specified timeframe
        
        Args:
            start_year (int): Starting year of data collection
            end_year (int): Ending year of data collection
            frequency (str): Frequency of data ('A' for annual)
        """
        years = ",".join([str(year) for year in range(start_year, end_year + 1)])
        
        params = {
            "UserID": self.api_key,
            "method": "GetData",
            "datasetname": "Regional",
            "TableName": "CAINC6N",  # Compensation of employees by NAICS industry
            "GeoFips": "COUNTY",  # All counties
            "Frequency": frequency,
            "Year": years,
            "ResultFormat": "JSON"
        }
        
        print(f"Requesting County Compensation by Industry data for years {start_year}-{end_year}...")
        response = requests.get(self.base_url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            # Save raw response for debugging
            raw_file = f"{self.output_dir}/raw_county_compensation_by_industry_{start_year}_{end_year}_{frequency}.json"
            with open(raw_file, 'w') as f:
                json.dump(data, f, indent=4)
            print(f"Saved raw Compensation by Industry response to {raw_file}")
            
            # Check for error messages in the response
            if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
                if 'Error' in data['BEAAPI']['Results']:
                    error = data['BEAAPI']['Results']['Error']
                    print(f"API Error: {error}")
                    return None
                
                if 'Data' in data['BEAAPI']['Results']:
                    df = pd.DataFrame(data['BEAAPI']['Results']['Data'])
                    filename = f"{self.output_dir}/county_compensation_by_industry_{start_year}_{end_year}_{frequency}.csv"
                    df.to_csv(filename, index=False)
                    print(f"Saved Compensation by Industry data to {filename}")
                    return df
                else:
                    print("No data found in API response")
                    return pd.DataFrame()
            else:
                print("Unexpected API response structure")
                return pd.DataFrame()
        else:
            print(f"Error {response.status_code}: {response.text}")
            return None
    
    def get_county_economic_profile(self, start_year, end_year, frequency="A"):
        """
        Get economic profile data for all counties for the specified timeframe
        
        Args:
            start_year (int): Starting year of data collection
            end_year (int): Ending year of data collection
            frequency (str): Frequency of data ('A' for annual)
        """
        years = ",".join([str(year) for year in range(start_year, end_year + 1)])
        
        params = {
            "UserID": self.api_key,
            "method": "GetData",
            "datasetname": "Regional",
            "TableName": "CAINC30",  # Economic profile
            "GeoFips": "COUNTY",  # All counties
            "Frequency": frequency,
            "Year": years,
            "ResultFormat": "JSON"
        }
        
        print(f"Requesting County Economic Profile data for years {start_year}-{end_year}...")
        response = requests.get(self.base_url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            # Save raw response for debugging
            raw_file = f"{self.output_dir}/raw_county_economic_profile_{start_year}_{end_year}_{frequency}.json"
            with open(raw_file, 'w') as f:
                json.dump(data, f, indent=4)
            print(f"Saved raw Economic Profile response to {raw_file}")
            
            # Check for error messages in the response
            if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
                if 'Error' in data['BEAAPI']['Results']:
                    error = data['BEAAPI']['Results']['Error']
                    print(f"API Error: {error}")
                    return None
                
                if 'Data' in data['BEAAPI']['Results']:
                    df = pd.DataFrame(data['BEAAPI']['Results']['Data'])
                    filename = f"{self.output_dir}/county_economic_profile_{start_year}_{end_year}_{frequency}.csv"
                    df.to_csv(filename, index=False)
                    print(f"Saved Economic Profile data to {filename}")
                    return df
                else:
                    print("No data found in API response")
                    return pd.DataFrame()
            else:
                print("Unexpected API response structure")
                return pd.DataFrame()
        else:
            print(f"Error {response.status_code}: {response.text}")
            return None
    
    def create_data_dictionary(self):
        """
        Create a data dictionary file with descriptions of all the fields
        collected from the BEA API.
        """
        # Get parameter metadata
        regional_params = self.get_parameters_for_dataset("Regional")
        
        data_dictionary = {
            "Tables": {
                "CAGDP9": "Real GDP by county and metropolitan area",
                "CAGDP2": "Gross domestic product (GDP) by county and metropolitan area by industry",
                "CAINC1": "County and MSA personal income summary: personal income, population, per capita personal income",
                "CAINC5N": "Personal income by major component and earnings by NAICS industry",
                "CAINC6N": "Compensation of employees by NAICS industry",
                "CAINC30": "Economic profile"
            },
            "CommonFields": {
                "GeoFips": "Geographic FIPS code for the county",
                "GeoName": "Geographic name of the county",
                "TimePeriod": "Time period for the data point (e.g., 2020 for year 2020)",
                "CL_UNIT": "Classification of the unit of measure",
                "UNIT_MULT": "The multiplier for the data value",
                "DataValue": "The actual data value",
                "NoteRef": "Reference to any notes applicable to the data point"
            },
            "SpecificFields": {
                "CAGDP9": {
                    "LineCode": "Line code for different types of GDP measures (1 is All industries total)",
                    "IndustryClassification": "Classification system for industries (typically NAICS)"
                },
                "CAGDP2": {
                    "LineCode": "Line code for different industry categories",
                    "IndustryClassification": "Classification system for industries (typically NAICS)"
                },
                "CAINC1": {
                    "LineCode": "Line code for different income categories (1: Personal income, 2: Population, 3: Per capita personal income)"
                },
                "CAINC5N": {
                    "LineCode": "Line code for different industry categories and components",
                    "IndustryClassification": "Classification system for industries (NAICS)"
                },
                "CAINC6N": {
                    "LineCode": "Line code for different industry categories",
                    "IndustryClassification": "Classification system for industries (NAICS)"
                },
                "CAINC30": {
                    "LineCode": "Line code for different economic indicators",
                    "Description": "Description of the economic indicator"
                }
            },
            "LineCodeDescriptions": {
                "CAINC1": {
                    "1": "Personal income (thousands of dollars)",
                    "2": "Population (persons)",
                    "3": "Per capita personal income (dollars)"
                }
                # More line code descriptions can be added
            },
            "Frequency": {
                "A": "Annual data"
            },
            "DatasetDescription": "Bureau of Economic Analysis (BEA) Regional Economic Accounts data for counties. This dataset includes economic indicators such as GDP, personal income, employment by industry, and other metrics for comparing economic conditions across counties. The data can be aggregated to MSA level for metropolitan analysis."
        }
        
        # Save data dictionary as JSON
        dictionary_file = f"{self.output_dir}/data_dictionary.json"
        with open(dictionary_file, 'w') as f:
            json.dump(data_dictionary, f, indent=4)
        
        print(f"Created data dictionary at {dictionary_file}")
        
        # Also create a more human-readable markdown version
        md_file = f"{self.output_dir}/data_dictionary.md"
        with open(md_file, 'w') as f:
            f.write("# BEA Regional Economic Accounts Data Dictionary\n\n")
            f.write(f"*Generated on {datetime.now().strftime('%Y-%m-%d')}*\n\n")
            
            f.write("## Dataset Description\n\n")
            f.write(f"{data_dictionary['DatasetDescription']}\n\n")
            
            f.write("## Tables\n\n")
            for table_id, description in data_dictionary['Tables'].items():
                f.write(f"- **{table_id}**: {description}\n")
            f.write("\n")
            
            f.write("## Common Fields\n\n")
            f.write("These fields appear in most or all data tables:\n\n")
            for field, desc in data_dictionary['CommonFields'].items():
                f.write(f"- **{field}**: {desc}\n")
            f.write("\n")
            
            f.write("## Table-Specific Fields\n\n")
            for table, fields in data_dictionary['SpecificFields'].items():
                f.write(f"### {table} ({data_dictionary['Tables'][table]})\n\n")
                for field, desc in fields.items():
                    f.write(f"- **{field}**: {desc}\n")
                f.write("\n")
            
            f.write("## Line Code Descriptions\n\n")
            for table, codes in data_dictionary['LineCodeDescriptions'].items():
                f.write(f"### {table} ({data_dictionary['Tables'][table]})\n\n")
                for code, desc in codes.items():
                    f.write(f"- **{code}**: {desc}\n")
                f.write("\n")
            
            f.write("## Frequency Codes\n\n")
            for code, desc in data_dictionary['Frequency'].items():
                f.write(f"- **{code}**: {desc}\n")
            
        print(f"Created human-readable data dictionary at {md_file}")


def main():
    """Main function to execute the BEA data collection"""
    # Import API key from secrets file
    try:
        from secrets import BEA_API_KEY
        api_key = BEA_API_KEY
    except ImportError:
        print("ERROR: secrets.py file not found. Please create it from secrets_template.py")
        return
    
    # Initialize the collector
    collector = BEADataCollector(api_key)
    
    # Set the date range for the last 5 years (adjust start_year as needed)
    current_year = datetime.now().year
    start_year = current_year - 5
    end_year = current_year - 1  # Previous complete year
    
    # First, let's get the dataset list to confirm what's available
    collector.get_dataset_list()
    
    # Get the parameters for the Regional dataset
    collector.get_parameters_for_dataset("Regional")
    
    # Get the available table names in the Regional dataset
    collector.get_parameter_values("Regional", "TableName")
    
    # Create data dictionary
    collector.create_data_dictionary()
    
    try:
        # Start with a smaller date range to test (2 years)
        test_start_year = end_year - 1
        
        print("\nCollecting test data for years", test_start_year, "to", end_year)
        
        # Collect total GDP data (annual)
        collector.get_county_gdp_data(test_start_year, end_year, frequency="A")
        # Collect GDP by industry data (annual)
        collector.get_county_gdp_by_industry(test_start_year, end_year, frequency="A")
        
        # Collect personal income data (annual)
        collector.get_county_personal_income(test_start_year, end_year, frequency="A")
        
        # Collect population data (annual)
        collector.get_county_population(test_start_year, end_year, frequency="A")
        
        # Collect per capita income data (annual)
        collector.get_county_per_capita_income(test_start_year, end_year, frequency="A")
        
        # Collect income by industry data (annual)
        collector.get_county_income_by_industry(test_start_year, end_year, frequency="A")
        
        # Collect compensation by industry data (annual)
        collector.get_county_compensation_by_industry(test_start_year, end_year, frequency="A")
        
        # Collect economic profile data (annual)
        collector.get_county_economic_profile(test_start_year, end_year, frequency="A")
        
        print("Test data collection complete!")
        
        # If the test is successful, proceed with the full 5-year range
        print("\nNow collecting full 5-year data range...")
        
        # Collect total GDP data (annual)
        collector.get_county_gdp_data(start_year, end_year, frequency="A")
        
        # Collect GDP by industry data (annual)
        collector.get_county_gdp_by_industry(start_year, end_year, frequency="A")
        
        # Collect personal income data (annual)
        collector.get_county_personal_income(start_year, end_year, frequency="A")
        
        # Collect population data (annual)
        collector.get_county_population(start_year, end_year, frequency="A")
        
        # Collect per capita income data (annual)
        collector.get_county_per_capita_income(start_year, end_year, frequency="A")
        
        # Collect income by industry data (annual)
        collector.get_county_income_by_industry(start_year, end_year, frequency="A")
        
        # Collect compensation by industry data (annual)
        collector.get_county_compensation_by_industry(start_year, end_year, frequency="A")
        
        # Collect economic profile data (annual)
        collector.get_county_economic_profile(start_year, end_year, frequency="A")
        
        print("Full BEA county data collection complete!")
        
    except Exception as e:
        print(f"An error occurred during data collection: {e}")


if __name__ == "__main__":
    main()