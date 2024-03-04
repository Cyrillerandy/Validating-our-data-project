#!/usr/bin/env python
# coding: utf-8

# In[ ]:


"""Field Data Processor

This module allows us to clean and process the data we extracted from our SQL database.

It requires that `pandas` and logging libraries be installed and executed within the pyhton environment as it runs.
It also requires that we import the previusoly created data ingestion module and its elements. 

It contains a class object with the following elements:
        * config_params - a dictionary with all needed variables like db_path, the csv urls and dataframe elements
        * initialize_logging - a method to initialize logging in the class
        * ingest_sql_data - a method to establish a connection with and load the SQL database into a dataframe
        * rename_columns - a method to rename the incorrectly assigned columns in the dataframe
        * apply_corrections - a method to apply corrections to the columns with errors in the dataframe
        * weather_station_mapping - a method to extract a csv file into a dataframe and merge it with the SQL dataframe
        * process - a method to call all the other methods at once for faster, easier processing
"""

import pandas as pd
from data_ingestion import create_db_engine, query_data, read_from_web_CSV
import logging

### START FUNCTION

class FieldDataProcessor:
    """
    A class used to clean up and process the field data
    
    ...
    
    Attributes:
        config_params (dict) :  A dictionary containing all the necessary variables for the module
        logging_level (str) : A string representing the level of logging to be utilised
        
    Methods:
        initialize_logging(logging_level) : 
            initializes/sets up  logging for an instance of the class object
            
        ingest_sql_data():
            Creates a connection to the SQL engine and connects to a database, loading it into a dataframe
        
        rename_columns():
            Renames incorrectly named columns in the loaded dataframe
            
        apply_corrections():
            Makes corrections to various errors in affected columns namely corrects spelling errors and incorrect negative values
            
        weather_station_mapping():
            Loads the weather station data from a csv file and merges it with the SQL dataframe
            
        process():
            Calls all the other methods at once for faster execution and less code
    """

    def __init__(self, config_params, logging_level="INFO"):  # Make sure to add this line, passing in config_params to the class 
        """Initializes the class 
        
        Args:
            config_params (dict) : Dictionary of all needed variables
            logging_level (str) : Establishes the logging level to be used
        """
        self.db_path = config_params['db_path']
        self.sql_query = config_params['sql_query']
        self.columns_to_rename = config_params['columns_to_rename']
        self.values_to_rename = config_params['values_to_rename']
        self.weather_map_data = config_params['weather_mapping_csv']

        # Add the rest of your class code here
        self.initialize_logging(logging_level)

        # We create empty objects to store the DataFrame and engine in
        self.df = None
        self.engine = None
        
    # This method enables logging in the class.
    def initialize_logging(self, logging_level):
        """
        Sets up logging for this instance of FieldDataProcessor.
        
        Args:
            logging_level (str) : The logging level used
        """
        logger_name = __name__ + ".FieldDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False  # Prevents log messages from being propagated to the root logger

        # Set logging level
        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":  # Option to disable logging
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO  # Default to INFO

        self.logger.setLevel(log_level)

        # Only add handler if not already added to avoid duplicate messages
        if not self.logger.handlers:
            ch = logging.StreamHandler()  # Create console handler
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

        # Use self.logger.info(), self.logger.debug(), etc.


    # let's focus only on this part from now on
    def ingest_sql_data(self):
        """
        Creates a database connection and loads the data into a dataframe
        
        Returns:
            DataFrame : A dataframe with the loaded database data
        """
        self.engine = create_db_engine(self.db_path)
        self.df = query_data(self.engine, self.sql_query)
        self.logger.info("Sucessfully loaded data.")
        return self.df

    
    def rename_columns(self):
        """
        Renames the wrongly named columns to their correct names
        """
        # Extract the columns to rename from the configuration
        column1, column2 = list(self.columns_to_rename.keys())[0], list(self.columns_to_rename.values())[0]  
        
        # Temporarily rename one of the columns to avoid a naming conflict
        temp_name = "__temp_name_for_swap__"
        while temp_name in self.df.columns:
            temp_name += "_"

        # Perform the swap
        self.df = self.df.rename(columns={column1: temp_name, column2: column1})
        self.df = self.df.rename(columns={temp_name: column2})
          
        self.logger.info(f"Swapped columns: {column1} with {column2}")
    
    def apply_corrections(self, column_name='Crop_type', abs_column='Elevation'):
        """
        Corrects errors in the specified dataframe columns
        """
        self.df[abs_column] = self.df[abs_column].abs()
        self.df[column_name] = self.df[column_name].apply(lambda crop: self.values_to_rename.get(crop, crop))
        self.df[column_name] = self.df[column_name].str.strip()


    def weather_station_mapping(self):
        """
        Loads the csv weather data into a dataframe and merges it to the SQL dataframe
        """
        self.df = self.df.merge(read_from_web_CSV(self.weather_map_data), on = 'Field_ID', how = 'left')
        
    def process(self):
        """
        Calls the other methods to perform all the operations of loading the data, cleaning and 
        correcting it and merging the dataframes 
        """
        self.ingest_sql_data()
        #Insert your code here 
        self.rename_columns()
        self.apply_corrections()
        self.weather_station_mapping()
        self.df = self.df.drop(columns="Unnamed: 0")
        
### END FUNCTION

