import snowflake.connector
#import pandas as pd

# Snowflake connection details
snowflake_env_details = {
    "user": "HIRING",
    "password": "d8ae57a3-c1ae-4c07-bf01-109c07b7595a",
    "account": "qnpxahj-tu59527.snowflakecomputing.com",
    "warehouse": "COMPUTE_WH",
    "database": "HIRING",
    "schema": "SCRATCH",
    "role": "HIRING_ROLE"
}

# Initialize conn as None to avoid NameError
conn = None

# Connect to Snowflake
try:
    conn = snowflake.connector.connect(**snowflake_env_details)
    print('Connected to Snowflake')

    cursor = conn.cursor()

    # File path where you stored
    file_path = "/Users/ajitpani/Documents/Snowflake_python_code/events.parquet"

    # Step 1: Create or  Parquet file format
    cursor.execute("""
        CREATE OR REPLACE FILE FORMAT ajit_parquet_format 
        TYPE = 'PARQUET';
    """)

    # Step 2: Create the stage
    cursor.execute("""
        CREATE OR REPLACE STAGE ajit_stage 
        FILE_FORMAT = ajit_parquet_format;
    """)

    # Step 3: Upload the Parquet file to the stage
    cursor.execute(f"PUT file://{file_path} @ajit_stage AUTO_COMPRESS = TRUE")

    # Step 4: Copy data from the stage into the Snowflake table
    cursor.execute("""
        COPY INTO HIRING.SCRATCH.RAW_EVENTS_AJIT
        FROM @ajit_stage
        FILE_FORMAT = (TYPE = 'PARQUET')
        ON_ERROR = 'CONTINUE'; 
    """)

    print("Data loaded successfully into HIRING.SCRATCH.RAW_EVENTS_AJIT!")

except snowflake.connector.errors.DatabaseError as e:
    print(f"DatabaseError: {e}")
except Exception as e:
    print(f"Error: {e}")

finally:
    # Ensure the cursor and connection are properly closed
    if conn:
        try:
            conn.close()
            print("Connection closed.")
        except Exception as e:
            print(f"Error while closing connection: {e}")