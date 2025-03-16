import requests
import snowflake.connector
import json
import time


# import schedule

# Initialize API with your label
def initialize_api(label):
    url = 'https://neonblue--nblearn-hiring-init.modal.run'
    headers = {"Content-Type": "application/json"}
    payload = json.dumps({"label": label})

    response = requests.post(url, headers=headers, data=payload)

    if response.status_code == 200:
        print(f"API Initialized successfully with label {label}")
    else:
        print("Failed to initialize API", response.status_code, response.text)


# Fetch data from API
def fetch_data(label, page_number=0, row_count=1000):
    url = 'https://neonblue--nblearn-hiring-read.modal.run'
    headers = {"Content-Type": "application/json"}
    payload = json.dumps({
        "label": label,
        "page_number": page_number,
        "row_count": row_count
    })

    response = requests.get(url, headers=headers, data=payload)

    if response.status_code == 200:
        return response.json()  # Assuming the response is in JSON format
    else:
        print("Failed to fetch data", response.status_code, response.text)
        return None


# Fetch all pages of data
def fetch_all_data(label):
    all_data = []
    page_number = 0
    row_count = 1000

    while True:
        data = fetch_data(label, page_number, row_count)
        if data:
            if not data:
                break  # No more data to fetch
            all_data.extend(data)  # Append new data
            page_number += 1
        else:
            break

    return all_data


# Connect to Snowflake
def connect_to_snowflake():
    return snowflake.connector.connect(
        user='HIRING',
        password='d8ae57a3-c1ae-4c07-bf01-109c07b7595a',
        account='qnpxahj-tu59527.snowflakecomputing.com',
        warehouse='COMPUTE_WH',
        database='HIRING',
        role='HIRING_ROLE',
        schema='SCRATCH'
    )


# Append new data to Snowflake table
def append_to_snowflake(data):
    conn = connect_to_snowflake()
    cursor = conn.cursor()

    for record in data:
        # Construct an INSERT SQL query based on your table structure
        # Assuming the table is structured with columns "column1", "column2", etc.
        sql_query = f"""
            INSERT INTO HIRING.SCRATCH.RAW_EVENTS_AJIT (RAW_EVENTS)
            VALUES (%s)
        """
        cursor.execute(sql_query, (record['field1']))

    cursor.close()
    conn.commit()
    conn.close()


# Define the task to poll API and append to Snowflake every hour
def task_to_run():
    label = "ajit-test4"  # Update with your label
    print("Fetching new data...")
    new_data = fetch_all_data(label)
    if new_data:
        print(f"Retrieved {len(new_data)} records. Appending to Snowflake...")
        print(new_data)
        append_to_snowflake(new_data)  # This insert data into snowflake
    else:
        print("No new data to append.")


# Schedule the task to run every hour
# schedule.every(1).hour.do(task_to_run)

# Initialize the API once
initialize_api("ajit-test4")  # Replace with your label

# Run the scheduler un commend once snowflake issues are fixed
# while True:
#    schedule.run_pending()
#    time.sleep(60)  # Check for pending tasks every minute
