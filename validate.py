# validate_data.py

import pandas as pd
import pytest
from data_ingestion import *
from field_data_processor import *
from weather_data_processor import *

config_params = {"sql_query": """
     SELECT *
     FROM geographic_features
     LEFT JOIN weather_features USING (Field_ID)
     LEFT JOIN soil_and_crop_features USING (Field_ID)
     LEFT JOIN farm_management_features USING (Field_ID)
     """, # Insert your SQL query
    "db_path": "sqlite:///Maji_Ndogo_farm_survey_small.db", # Insert the db_path of the database
    "columns_to_rename": {'Annual_yield': 'Crop_type', 'Crop_type': 'Annual_yield'}, # Insert the dictionary of columns we want to swop the names of, 
    "values_to_rename": {'cassaval': 'cassava', 'wheatn': 'wheat', 'teaa': 'tea'}, # Insert the croptype renaming dictionary
    "weather_csv_path": "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_station_data.csv", # Insert the weather data CSV here
    "weather_mapping_csv": "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_data_field_mapping.csv", # Insert the weather data mapping CSV here
    # Add two new keys
   "weather_csv_path":  "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_station_data.csv", # Insert the URL for the weather station data
    "regex_patterns" : {'Rainfall': r'(\d+(\.\d+)?)\s?mm',
                        'Temperature': r'(\d+(\.\d+)?)\s?C',
                        'Pollution_level': r'=\s*(-?\d+(\.\d+)?)|Pollution at \s*(-?\d+(\.\d+)?)'} # Insert the regex pattern we used to process the messages
}

def read_weather_data():
    # Load weather data, replace 'sampled_weather_df.csv' with your actual file
    weather_data = pd.read_csv(config_params['weather_csv_path'])
    return weather_data

def read_field_data():
    # Load field data, replace 'sampled_field_df.csv' with your actual file
    field_data = query_data(create_db_engine(config_params['db_path'], config_params['sql_query'])
    return field_data

def test_read_weather_DataFrame_shape():
    weather_data = read_weather_data()
    expected_rows, expected_columns = 1843, 2  # Replace with your expected values
    assert weather_data.shape == (expected_rows, expected_columns)

def test_read_field_DataFrame_shape():
    field_data = read_field_data()
    expected_rows, expected_columns = 5654, 18  # Replace with your expected values
    assert field_data.shape == (expected_rows, expected_columns)

def test_weather_DataFrame_columns():
    weather_data = read_weather_data()
    expected_columns = ['Weather_station_ID', 'Message']  # Replace with your expected column names
    assert list(weather_data.columns) == expected_columns

def test_field_DataFrame_columns():
    field_data = read_field_data()
    expected_columns = ['Field_ID', 'Elevation', 'Latitude', 'Longitude', 'Location', 'Slope',
       'Rainfall', 'Min_temperature_C', 'Max_temperature_C', 'Ave_temps',
       'Soil_fertility', 'Soil_type', 'pH', 'Pollution_level', 'Plot_size',
       'Annual_yield', 'Crop_type', 'Standard_yield']  # Replace with your expected column names
    assert list(field_data.columns) == expected_columns

def test_field_DataFrame_non_negative_elevation():
    field_data = read_field_data()
    assert field_data['Elevation'].values >= 0 
                            
def test_crop_types_are_valid():
    field_data = read_field_data()
    assert field_data['Crop_type'] in ['cassava', 'tea', 'wheat', 'potato', 'banana', 'coffee', 'rice', 'maize']
                            
def test_positive_rainfall_values():
    pass
                            
                            
# Add more tests based on your specific validation requirements

if __name__ == "__main__":
    pytest.main(['validate_data.py', '-v'])