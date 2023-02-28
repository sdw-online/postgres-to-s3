import boto3
from dotenv import load_dotenv
import os 
import json
import pandas as pd
import io
import psycopg2
import logging, coloredlogs
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
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
schema_name                         =      'main'
db_name                             =       database
table_1                             =       ''
table_2                             =       ''
table_3                             =       ''


get_raw_tables_from_postgres_dwh_sql                         =      text(f'''SELECT table_name FROM information_schema.tables
                                                                        WHERE table_type = 'BASE TABLE'
                                                                        AND table_schema = '{schema_name}'
                                                                        ;   ''')
sql_query_2                         =      f'''SELECT * FROM {schema_name}.{table_2} ;   '''
sql_query_3                         =      f'''SELECT * FROM {schema_name}.{table_3} ;   '''
        
postgres_connection = psycopg2.connect(
                host        =   host,
                port        =   port,
                dbname      =   database,
                user        =   username,
                password    =   password,
        )
postgres_connection.set_session(autocommit=True)

try:
     
    # Validate the Postgres database connection
    if postgres_connection.closed == 0:
        root_logger.debug(f"")
        root_logger.info("=================================================================================")
        root_logger.info(f"CONNECTION SUCCESS: Managed to connect successfully to the '{db_name}' database!!")
        root_logger.info(f"Connection details: '{postgres_connection.dsn}' ")
        root_logger.info("=================================================================================")
        root_logger.debug("")
    
    elif postgres_connection.closed != 0:
        raise ConnectionError(f"CONNECTION ERROR: Unable to connect to the '{db_name}' database...") 
    

    # Create a cursor object to execute the PG-SQL commands 
    cursor      =   postgres_connection.cursor()



    # Get tables 
    Session = scoped_session(sessionmaker(bind=sql_alchemy_engine))
    s = Session()
    raw_tables = s.execute(get_raw_tables_from_postgres_dwh_sql)


    for raw_table in raw_tables:
        df = pd.read_sql(text(f'SELECT * FROM {raw_table};'), con=sql_alchemy_engine)
        root_logger.debug(df.head(3))
        root_logger.info(f'')

except psycopg2.Error as e:
    print(e)