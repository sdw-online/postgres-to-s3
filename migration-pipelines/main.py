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

ACCESS_KEY              =   os.getenv("ACCESS_KEY")
SECRET_ACCESS_KEY       =   os.getenv("SECRET_ACCESS_KEY")
REGION_NAME             =   os.getenv("REGION_NAME")
S3_BUCKET               =   os.getenv("S3_BUCKET")


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


# raw_json_filepath = os.getenv("DATA_LOCATION")



# Set up SQL statements 
get_raw_tables_from_postgres_dwh_sql                         =      f'''SELECT table_name FROM information_schema.tables
                                                                        WHERE table_type = 'BASE TABLE'
                                                                        AND table_schema = '{schema_name}'
                                                                        ;   
'''

count_total_no_of_postgres_raw_tables = f'''        SELECT table_name FROM information_schema.tables 
                                                        WHERE table_type = 'BASE TABLE' AND table_schema = '{schema_name}';
'''


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




def load_raw_data_from_postgres_to_s3(postgres_connection):
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

        root_logger.info("")
        root_logger.info("---------------------------------------------")
        root_logger.info("Now extracting data from the Postgres data warehouse raw layer...")

        cursor.execute(get_raw_tables_from_postgres_dwh_sql)
        raw_tables = cursor.fetchall()
        tables_imported_to_s3 = 0


        for raw_table in raw_tables:
            
            cursor.execute(f'SELECT * FROM {schema_name}.{raw_table[0]} ')
            sql_results = cursor.fetchall()
            tables_imported_to_s3 += 1
            df = pd.DataFrame(data=sql_results, columns=[desc[0] for desc in cursor.description])
            root_logger.debug(f'Raw table name: {raw_table[0]}')
            root_logger.debug(df.head(3))
            root_logger.info(f'')

            root_logger.info(f"Importing '{raw_table[0]}' table to S3 bucket as JSON file...  ") 

            # Set up constants for S3 file to be imported
            s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_ACCESS_KEY, region_name=REGION_NAME)
            raw_table_csv_file = f'{raw_table[0]}.csv'
            raw_sub_folder = 'raw_folder/'
            S3_KEY = raw_sub_folder + raw_table_csv_file

            with io.StringIO() as csv_parser:
                raw_df_to_csv = df.to_csv(raw_table_csv_file, index=False)
                RAW_DATA_CSV_BODY = csv_parser.getvalue()


                # Load Postgres table to S3
                s3.put_object(Bucket=S3_BUCKET,
                            Key=S3_KEY,
                            Body=RAW_DATA_CSV_BODY
                            )
                root_logger.info(f"Successfully loaded '{S3_KEY}' file to the '{S3_BUCKET}' S3 bucket... ")
                root_logger.info("")
                root_logger.info(f"     --- {tables_imported_to_s3}/{len(raw_tables)} raw tables in S3... ")
                root_logger.info("")
                root_logger.info("---------------------------------------------")



        root_logger.info("")
        root_logger.info("---------------------------------------------")
        root_logger.info("Successfully extracted the data from the Postgres data warehouse raw layer . Now advancing to the next stage... ")

    except psycopg2.Error as e:
        root_logger.error(f'ERROR IN EXTRACTING OR LOADING DATA: {str(e)} ')


    finally:
        
        # Close the cursor if it exists 
        if cursor is not None:
            cursor.close()
            root_logger.debug("")
            root_logger.debug("Cursor closed successfully.")

        # Close the database connection to Postgres if it exists 
        if postgres_connection is not None:
            postgres_connection.close()
            # root_logger.debug("")
            root_logger.debug("Session connected to Postgres database closed.")



def perform_import_validation_checks(postgres_connection):
    
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



        # Check total number of rows and columns in Postgres tables for raw layer

        root_logger.info("")
        root_logger.info("---------------------------------------------")
        root_logger.info("Preparing row & column count job on raw tables in Postgres data warehouse ...")
        root_logger.debug("")
        root_logger.debug("")
        root_logger.info(f'')
        root_logger.info(f'')
        root_logger.info('================================================')
        root_logger.info('           DATA PROFILING METRICS: POSTGRES             ')
        root_logger.info('================================================')
        root_logger.info(f'')
        root_logger.info(f'Now calculating Postgres raw tables statistics...')
        root_logger.info(f'')
        root_logger.info(f'')




        cursor.execute(count_total_no_of_postgres_raw_tables)
        raw_tables = cursor.fetchall()

        for raw_table in raw_tables:
            count_total_no_of_rows_in_postgres_table   =   f'''   SELECT COUNT(*) FROM {schema_name}.{raw_table[0]} ;
            '''
            count_total_no_of_columns_in_postgres_table  =   f'''            SELECT          COUNT(column_name) 
                                                                FROM            information_schema.columns 
                                                                WHERE           table_name      =   '{raw_table[0]}'
                                                                AND             table_schema    =   '{schema_name}'
            '''



            cursor.execute(count_total_no_of_rows_in_postgres_table)
            sql_result_for_row_count = cursor.fetchone()[0]

            cursor.execute(count_total_no_of_columns_in_postgres_table)
            sql_result_for_column_count = cursor.fetchone()[0]



            # root_logger.info("")
            root_logger.info(f'Postgres table name:                         {raw_table[0]} ')
            root_logger.info(f'Number of rows in Postgres table:            {sql_result_for_row_count} ')
            root_logger.info(f'Number of columns in Postgres table:         {sql_result_for_column_count} ')
            root_logger.info("")
            root_logger.info("---------------------------------------------")
            root_logger.info("")



        # Check total number of rows and columns in Amazon Athena tables for raw layer


        athena_client = ''







    except psycopg2.Error as e:
        root_logger.error(f'ERROR IN PERFORMING VALIDATION CHECKS: {str(e)} ')


    finally:
        
        # Close the cursor if it exists 
        if cursor is not None:
            cursor.close()
            root_logger.debug("")
            root_logger.debug("Cursor closed successfully.")

        # Close the database connection to Postgres if it exists 
        if postgres_connection is not None:
            postgres_connection.close()
            # root_logger.debug("")
            root_logger.debug("Session connected to Postgres database closed.")





load_raw_data_from_postgres_to_s3(postgres_connection)


perform_import_validation_checks(postgres_connection)
