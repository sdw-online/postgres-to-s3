import boto3
from dotenv import load_dotenv
import os 
import json
import pandas as pd
import io
import psycopg2
import logging, coloredlogs
from sqlalchemy import create_engine
from pathlib import Path

# ================================================ LOGGER ================================================


# Set up root root_logger 
root_logger     =   logging.getLogger(__name__)
root_logger.setLevel(logging.DEBUG)


# Set up formatter for logs 
file_handler_log_formatter      =   logging.Formatter('%(asctime)s  |  %(levelname)s  |  %(message)s  ')
console_handler_log_formatter   =   coloredlogs.ColoredFormatter(fmt    =   '%(message)s', level_styles=dict(
                                                                                                debug           =   dict    (color  =   'white'),
                                                                                                info            =   dict    (color  =   'green'),
                                                                                                warning         =   dict    (color  =   'cyan'),
                                                                                                error           =   dict    (color  =   'red',      bold    =   True,   bright      =   True),
                                                                                                critical        =   dict    (color  =   'black',    bold    =   True,   background  =   'red')
                                                                                            ),

                                                                                    field_styles=dict(
                                                                                        messages            =   dict    (color  =   'white')
                                                                                    )
                                                                                    )


# Set up file handler object for logging events to file
current_filepath    =   Path(__file__).stem
file_handler        =   logging.FileHandler('logs/config/' + current_filepath + '.log', mode='w')
file_handler.setFormatter(file_handler_log_formatter)


# Set up console handler object for writing event logs to console in real time (i.e. streams events to stderr)
console_handler     =   logging.StreamHandler()
console_handler.setFormatter(console_handler_log_formatter)


# Add the file handler 
root_logger.addHandler(file_handler)


# Only add the console handler if the script is running directly from this location 
if __name__=="__main__":
    root_logger.addHandler(console_handler)






# ================================================ CONFIG ================================================

# Load environment variables to session
load_dotenv()





# Set up connection to AWS 



# Set up connection to Postgres database 
 
host                    =   os.getenv("HOST")
port                    =   os.getenv('PORT')
database                =   os.getenv('RAW_DB')
username                =   os.getenv('PG_USERNAME')
password                =   os.getenv('PASSWORD')

postgres_connection     =   None
cursor                  =   None

sql_alchemy_engine                  =       create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')
active_schema_name                  =      'reporting'
active_db_name                      =       database
table_1                             =       ''
table_2                             =       ''
table_3                             =       ''
sql_query_1                         =      f'''SELECT * FROM {active_schema_name}.{table_1} ;   '''
sql_query_2                         =      f'''SELECT * FROM {active_schema_name}.{table_2} ;   '''
sql_query_3                         =      f'''SELECT * FROM {active_schema_name}.{table_3} ;   '''
        

try:

    engine = create_engine(f'{sql_alchemy_engine}')
     
    # Validate the Postgres database connection
    if postgres_connection.closed == 0:
        root_logger.debug(f"")
        root_logger.info("=================================================================================")
        root_logger.info(f"CONNECTION SUCCESS: Managed to connect successfully to the '{active_db_name}' database!!")
        root_logger.info(f"Connection details: '{postgres_connection.dsn}' ")
        root_logger.info("=================================================================================")
        root_logger.debug("")
    
    elif postgres_connection.closed != 0:
        raise ConnectionError(f"CONNECTION ERROR: Unable to connect to the '{active_db_name}' database...") 
    


except psycopg2.Error as e:
    print(e)