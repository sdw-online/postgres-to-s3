import boto3
from dotenv import load_dotenv
from configparser import ConfigParser
import os 
import json
import pandas as pd
import io
import psycopg2
from sqlalchemy import create_engine
from pathlib import Path



load_dotenv()

# Create a config file for storing environment variables
path    =   os.path.abspath('postgres-to-s3/config.ini')
config  =   ConfigParser()
config.read(path)



# Set up connection to AWS 



# Set up connection to Postgres database 
 
host                    =   os.getenv("HOST")
port                    =   os.getenv('PORT')
database                =   os.getenv('RAW_DB')
username                =   os.getenv('USERNAME')
password                =   os.getenv('PASSWORD')

postgres_connection     =   None
cursor                  =   None



print("SUCCESS!!!")