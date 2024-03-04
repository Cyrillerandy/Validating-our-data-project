#!/usr/bin/env python
# coding: utf-8

# In[ ]:


"""Data Ingestion Pipeline

This module allows us to be able to connect to a database via creation of an engine, query the database to generate
dataframes and read csv files into a dataframes for subsequent processing.

It requires that `pandas`, `sqlalchemy` and `logging` python libraries be installed and imported within the python 
environment when the script is running. 

The functions the file contains are:
        * create_db_engine - makes a connection to and creates a database engine 
        * query_data - utilises a sql query to read data from the database and create a dataframe
        * read_from_web_CSV - reads data from csv files into pandas dataframes
"""
from sqlalchemy import create_engine, text
import logging
import pandas as pd
# Name our logger so we know that logs from this module come from the data_ingestion module
logger = logging.getLogger('data_ingestion')
# Set a basic logging message up that prints out a timestamp, the name of our logger, and the message
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# In[ ]:


### START FUNCTION

def create_db_engine(db_path):
    """Makes a connection to a SQL database
    
    Args:
        db_path (str) : The URL of the database to connect with
        
    Returns:
        object : The engine object for the connection to the specified database
        error : In the event anything fails an error will be raised
    """
    try:
        engine = create_engine(db_path)
        # Test connection
        with engine.connect() as conn:
            pass
        # test if the database engine was created successfully
        logger.info("Database engine created successfully.")
        return engine # Return the engine object if it all works well
    except ImportError: #If we get an ImportError, inform the user SQLAlchemy is not installed
        logger.error("SQLAlchemy is required to use this function. Please install it first.")
        raise e
    except Exception as e:# If we fail to create an engine inform the user
        logger.error(f"Failed to create database engine. Error: {e}")
        raise e
    
def query_data(engine, sql_query):
    """Runs an sql query to retrieve data from the engine database
    
    Args:
        engine (object) : The sql engine database that was connected to
        sql_query (str) : The sql query that will be used to retrieve data from the database engine
        
    Returns:
        DataFrame : A pandas dataframe containing the data from the database
        error : In the event the function doesn't work well, errors will be raised
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(sql_query), connection)
        if df.empty:
            # Log a message or handle the empty DataFrame scenario as needed
            msg = "The query returned an empty DataFrame."
            logger.error(msg)
            raise ValueError(msg)
        logger.info("Query executed successfully.")
        return df
    except ValueError as e: 
        logger.error(f"SQL query failed. Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred while querying the database. Error: {e}")
        raise e
    
def read_from_web_CSV(URL):
    """Reads data from a csv file url
    
    Args:
        URL (str) : The URL of the csv file to be read
        
    Returns:
        DataFrame : A pandas dataframe containing the data retrieved from the csv file
        error : An error will be raised in the event the function doesn't work properly
    """
    try:
        df = pd.read_csv(URL)
        logger.info("CSV file read successfully from the web.")
        return df
    except pd.errors.EmptyDataError as e:
        logger.error("The URL does not point to a valid CSV file. Please check the URL and try again.")
        raise e
    except Exception as e:
        logger.error(f"Failed to read CSV from the web. Error: {e}")
        raise e
    
### END FUNCTION

