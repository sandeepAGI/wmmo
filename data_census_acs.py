import os
import requests
import pandas as pd
import json
from datetime import datetime
from pathlib import Path
import time

def collect_acs_data(api_key):
    """
    Collect ACS 5-year data for 2023 at the county level
    
    Parameters:
    -----------
    api_key : str
        Census Bureau API key
    
    Returns:
    --------
    pandas.DataFrame
        DataFrame with ACS data
    """
    # Constants
    BASE_URL = "https://api.census.gov/data"
    YEAR = 2023
    OUTPUT_DIR = Path("census_acs_data")
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    print(f"Collecting 2023 ACS 5-year data at the county level...")
    
    # Define core demographic, income, education, housing variables
    variables = [
        # Demographics
        'B01001_001E',  # Total population
        'B01002_001E',  # Median age
        
        # Age brackets for wealth accumulation years (45-64)
        'B01001_014E',  # Males 45 to 49 years
        'B01001_015E',  # Males 50 to 54 years
        'B01001_016E',  # Males 55 to 59 years
        'B01001_017E',  # Males 60 to 64 years
        'B01001_038E',  # Females 45 to 49 years
        'B01001_039E',  # Females 50 to 54 years
        'B01001_040E',  # Females 55 to 59 years
        'B01001_041E',  # Females 60 to 64 years
        
        # Income
        'B19001_001E',  # Total households
        'B19001_014E',  # Households with income $100,000 to $124,999
        'B19001_015E',  # Households with income $125,000 to $149,999
        'B19001_016E',  # Households with income $150,000 to $199,999
        'B19001_017E',  # Households with income $200,000 or more
        'B19013_001E',  # Median household income
        'B19301_001E',  # Per capita income
        'B19025_001E',  # Aggregate household income
        'B19083_001E',  # Gini index of income inequality
        
        # Education
        'B15003_001E',  # Total population 25 years and over
        'B15003_022E',  # Bachelor's degree
        'B15003_023E',  # Master's degree
        'B15003_024E',  # Professional school degree
        'B15003_025E',  # Doctorate degree
        
        # Housing
        'B25077_001E',  # Median value of owner-occupied housing units
        'B25075_001E',  # Total owner-occupied housing units
        'B25075_020E',  # Owner-occupied units worth $500,000 to $749,999
        'B25075_021E',  # Owner-occupied units worth $750,000 to $999,999
        'B25075_022E',  # Owner-occupied units worth $1,000,000 to $1,499,999
        'B25075_023E',  # Owner-occupied units worth $1,500,000 to $1,999,999
        'B25075_024E',  # Owner-occupied units worth $2,000,000 or more
        'B25095_001E',  # Aggregate value of owner-occupied housing units
    ]
    
    # Step 1: Validate API key with a simple request
    test_url = f"{BASE_URL}/{YEAR}/acs/acs5?get=NAME&for=state:01&key={api_key}"
    try:
        print("Validating API key...")
        response = requests.get(test_url)
        if "Invalid Key" in response.text:
            print("ERROR: Your Census API key appears to be invalid.")
            return None
        response.json()  # Check if response is valid JSON
        print("API key validated successfully.")
    except Exception as e:
        print(f"Error validating API key: {e}")
        return None
    
    # Step 2: Get list of states
    try:
        print("Getting list of states...")
        states_url = f"{BASE_URL}/{YEAR}/acs/acs5?get=NAME&for=state:*&key={api_key}"
        states_response = requests.get(states_url)
        states_response.raise_for_status()
        states_data = states_response.json()
        
        # Skip header row
        states = {}
        for row in states_data[1:]:
            state_name = row[0]
            state_id = row[1]
            states[state_id] = state_name
            
        print(f"Found {len(states)} states")
    except Exception as e:
        print(f"Error getting states: {e}")
        return None
    
    # Step 3: Collect county data for each state
    all_county_data = []
    
    for state_id, state_name in states.items():
        try:
            print(f"Collecting data for counties in {state_name} (State ID: {state_id})...")
            
            # Split variables into chunks to avoid URL length issues
            var_chunks = [variables[i:i+10] for i in range(0, len(variables), 10)]
            state_counties_data = []
            
            for chunk_idx, var_chunk in enumerate(var_chunks):
                # Create comma-separated variable list
                var_str = ','.join(['NAME'] + var_chunk)
                
                # Build API URL for counties in this state
                url = f"{BASE_URL}/{YEAR}/acs/acs5?get={var_str}&for=county:*&in=state:{state_id}&key={api_key}"
                
                try:
                    print(f"  Fetching chunk {chunk_idx+1}/{len(var_chunks)}...")
                    
                    response = requests.get(url)
                    response.raise_for_status()
                    
                    data = response.json()
                    headers = data[0]
                    
                    # Convert to DataFrame
                    df_chunk = pd.DataFrame(data[1:], columns=headers)
                    
                    if chunk_idx == 0:
                        # For first chunk, keep all columns
                        state_counties_data.append(df_chunk)
                    else:
                        # For subsequent chunks, only keep data variables
                        keep_cols = [col for col in df_chunk.columns if col in var_chunk]
                        state_counties_data.append(df_chunk[keep_cols])
                    
                    time.sleep(0.5)  # Be nice to the API
                    
                except Exception as e:
                    print(f"  Error fetching chunk {chunk_idx+1}: {e}")
                    print(f"  Response: {response.text[:200] if 'response' in locals() else 'N/A'}")
            
            # Combine chunks for this state
            if len(state_counties_data) > 0:
                if len(state_counties_data) == 1:
                    state_df = state_counties_data[0]
                else:
                    # Start with first chunk
                    state_df = state_counties_data[0]
                    
                    # Add columns from other chunks
                    for df_chunk in state_counties_data[1:]:
                        for col in df_chunk.columns:
                            if col in state_df.columns:
                                continue
                            state_df[col] = df_chunk[col]
                
                # Add to overall results
                all_county_data.append(state_df)
                print(f"  Added {len(state_df)} counties from {state_name}")
                
            # Be nice to the API
            time.sleep(1)
            
        except Exception as e:
            print(f"Error processing state {state_name}: {e}")
    
    # Step 4: Combine all county data
    if len(all_county_data) == 0:
        print("No data collected.")
        return None
    
    all_counties = pd.concat(all_county_data, ignore_index=True)
    
    # Step 5: Add extra information
    all_counties['year'] = YEAR
    
    # Save the data
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = OUTPUT_DIR / f"census_acs_county_data_{YEAR}_{timestamp}.csv"
    all_counties.to_csv(output_path, index=False)
    
    print(f"Data saved to {output_path}")
    print(f"Collected data for {len(all_counties)} counties with {len(all_counties.columns)} variables")
    
    # Step 6: Lookup Metropolitan Areas
    try:
        print("Looking up Core Based Statistical Areas (CBSAs)...")
        
        # Try a few variations of CBSA parameters to find one that works
        # Note: We're using a small state as a test case (Delaware, state:10)
        cbsa_formats = [
            "metropolitan%20statistical%20area:*&in=state:10",
            "metropolitan%20statistical%20area/micropolitan%20statistical%20area:*&in=state:10",
            "cbsa:*&in=state:10",
            "combined%20statistical%20area:*&in=state:10",
            "core%20based%20statistical%20area:*&in=state:10"
        ]
        
        cbsa_param = None
        cbsa_data = None
        
        for format_str in cbsa_formats:
            try:
                test_url = f"{BASE_URL}/{YEAR}/acs/acs5?get=NAME&for={format_str}&key={api_key}"
                print(f"Trying CBSA format: {test_url}")
                
                response = requests.get(test_url)
                
                # If we get a JSON response, this format works
                if response.status_code == 200:
                    try:
                        test_data = response.json()
                        if isinstance(test_data, list) and len(test_data) > 1:
                            cbsa_param = format_str.split('&')[0]  # Extract just the "for" part
                            cbsa_data = test_data
                            print(f"Found working CBSA format: {cbsa_param}")
                            break
                    except:
                        pass
                        
                print(f"  Format not working: {response.text[:200]}")
            except Exception as e:
                print(f"  Error with format '{format_str}': {e}")
        
        if cbsa_param:
            print(f"Successfully found CBSA parameter format. Will include in documentation.")
            # We'll store this for later but not process CBSA data in this version
            
            # Create documentation note for future use
            cbsa_note = f"""
            # Working CBSA Parameter Format for {YEAR}:
            # URL format: {BASE_URL}/{YEAR}/acs/acs5?get=NAME&for={cbsa_param}&key=YOUR_KEY
            """
            
            cbsa_doc_path = OUTPUT_DIR / f"cbsa_api_format_{YEAR}.txt"
            with open(cbsa_doc_path, 'w') as f:
                f.write(cbsa_note)
                
            print(f"CBSA parameter format saved to {cbsa_doc_path}")
        else:
            print("Could not find a working CBSA parameter format. Using county data only.")
    except Exception as e:
        print(f"Error looking up CBSA information: {e}")
    
    return all_counties

def create_data_dictionary(variables):
    """
    Create a simple data dictionary for the ACS variables
    
    Parameters:
    -----------
    variables : list
        List of variable codes
    
    Returns:
    --------
    pandas.DataFrame
        DataFrame with variable documentation
    """
    # Define variable groups and descriptions
    var_info = {
        # Demographics
        'B01001_001E': {'group': 'Demographics', 'label': 'Total population'},
        'B01002_001E': {'group': 'Demographics', 'label': 'Median age'},
        'B01001_014E': {'group': 'Demographics', 'label': 'Males 45 to 49 years'},
        'B01001_015E': {'group': 'Demographics', 'label': 'Males 50 to 54 years'},
        'B01001_016E': {'group': 'Demographics', 'label': 'Males 55 to 59 years'},
        'B01001_017E': {'group': 'Demographics', 'label': 'Males 60 to 64 years'},
        'B01001_038E': {'group': 'Demographics', 'label': 'Females 45 to 49 years'},
        'B01001_039E': {'group': 'Demographics', 'label': 'Females 50 to 54 years'},
        'B01001_040E': {'group': 'Demographics', 'label': 'Females 55 to 59 years'},
        'B01001_041E': {'group': 'Demographics', 'label': 'Females 60 to 64 years'},
        
        # Income
        'B19001_001E': {'group': 'Income', 'label': 'Total households'},
        'B19001_014E': {'group': 'Income', 'label': 'Households with income $100,000 to $124,999'},
        'B19001_015E': {'group': 'Income', 'label': 'Households with income $125,000 to $149,999'},
        'B19001_016E': {'group': 'Income', 'label': 'Households with income $150,000 to $199,999'},
        'B19001_017E': {'group': 'Income', 'label': 'Households with income $200,000 or more'},
        'B19013_001E': {'group': 'Income', 'label': 'Median household income'},
        'B19301_001E': {'group': 'Income', 'label': 'Per capita income'},
        'B19025_001E': {'group': 'Income', 'label': 'Aggregate household income'},
        'B19083_001E': {'group': 'Income', 'label': 'Gini index of income inequality'},
        
        # Education
        'B15003_001E': {'group': 'Education', 'label': 'Total population 25 years and over'},
        'B15003_022E': {'group': 'Education', 'label': 'Bachelor\'s degree'},
        'B15003_023E': {'group': 'Education', 'label': 'Master\'s degree'},
        'B15003_024E': {'group': 'Education', 'label': 'Professional school degree'},
        'B15003_025E': {'group': 'Education', 'label': 'Doctorate degree'},
        
        # Housing
        'B25077_001E': {'group': 'Housing', 'label': 'Median value of owner-occupied housing units'},
        'B25075_001E': {'group': 'Housing', 'label': 'Total owner-occupied housing units'},
        'B25075_020E': {'group': 'Housing', 'label': 'Owner-occupied units worth $500,000 to $749,999'},
        'B25075_021E': {'group': 'Housing', 'label': 'Owner-occupied units worth $750,000 to $999,999'},
        'B25075_022E': {'group': 'Housing', 'label': 'Owner-occupied units worth $1,000,000 to $1,499,999'},
        'B25075_023E': {'group': 'Housing', 'label': 'Owner-occupied units worth $1,500,000 to $1,999,999'},
        'B25075_024E': {'group': 'Housing', 'label': 'Owner-occupied units worth $2,000,000 or more'},
        'B25095_001E': {'group': 'Housing', 'label': 'Aggregate value of owner-occupied housing units'},
    }
    
    # Add metadata fields
    var_info.update({
        'year': {'group': 'Metadata', 'label': 'Year of data'},
        'state': {'group': 'Metadata', 'label': 'State FIPS code'},
        'county': {'group': 'Metadata', 'label': 'County FIPS code'},
        'NAME': {'group': 'Metadata', 'label': 'Geographic area name'},
    })
    
    # Create DataFrame with only the variables we have
    records = []
    for var_code in variables:
        if var_code in var_info:
            records.append({
                'variable_code': var_code,
                'group': var_info[var_code]['group'],
                'label': var_info[var_code]['label']
            })
        else:
            # For any variables not in our predefined list
            records.append({
                'variable_code': var_code,
                'group': 'Other',
                'label': f'Variable {var_code}'
            })
    
    return pd.DataFrame(records)

if __name__ == "__main__":
    # Import API key from secrets file
    try:
        from secrets import CENSUS_API_KEY
        API_KEY = CENSUS_API_KEY
    except ImportError:
        print("ERROR: secrets.py file not found. Please create it from secrets_template.py")
        exit(1)
    
    # Collect ACS data
    acs_data = collect_acs_data(API_KEY)
    
    if acs_data is not None:
        # Create and save data dictionary
        variables = [col for col in acs_data.columns if col not in ['year', 'state', 'county', 'NAME']]
        data_dict = create_data_dictionary(variables)
        
        output_dir = Path("census_acs_data")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dict_path = output_dir / f"census_acs_data_dictionary_{timestamp}.csv"
        data_dict.to_csv(dict_path, index=False)
        
        print(f"Data dictionary saved to {dict_path}")
        
        # Display sample of the data
        print("\nSample of ACS data:")
        print(acs_data.head())