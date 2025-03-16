import requests
import csv
import json
import time  # Ensure that this import is included
import os
from datetime import datetime

# Step 1: Convert the date to Unix timestamp with nanoseconds
def date_to_unix_timestamp_with_nanoseconds(date_string):
    dt_object = datetime.strptime(date_string, "%Y-%m-%d")
    timestamp_seconds = int(dt_object.timestamp())  # Convert to seconds
    timestamp_nanoseconds = timestamp_seconds * 1_000_000_000  # Convert to nanoseconds
    return timestamp_nanoseconds

# Step 2: Initialize the API session with a label
def initialize_session(label):
    url = "https://neonblue--nblearn-hiring-init.modal.run"
    headers = {"Content-Type": "application/json"}
    data = {"label": label}

    response = requests.post(url, json=data, headers=headers)

    if response.status_code == 200:
        print(f"Session initialized with label: {label}")
    else:
        print(f"Error initializing session: {response.status_code}")
        print(response.text)
        return None
    return response.json()

# Step 3: Fetch data from the API, passing the filter for "created_at" (date after 2024-01-01)
def fetch_data(label, page_number=0, row_count=1000, start_timestamp=None, end_timestamp=None):
    url = "https://neonblue--nblearn-hiring-read.modal.run"
    headers = {"Content-Type": "application/json"}

    # Include start timestamp and end timestamp for filtering if provided
    data = {
        "label": label,
        "page_number": page_number,
        "row_count": row_count
    }

    if start_timestamp:
        data["created_after"] = start_timestamp  # Filter for data after the start date
    if end_timestamp:
        data["created_before"] = end_timestamp  # Filter for data before the end date

    response = requests.get(url, json=data, headers=headers)

    print(f"Response Status Code: {response.status_code}")
    print(f"Response Content (first 500 chars): {response.text[:500]}")  # Debugging: print first 500 characters for inspection

    if response.status_code == 200:
        try:
            # Clean up response text (remove any stray characters)
            cleaned_response = response.text.strip()
            if cleaned_response.startswith("[") and cleaned_response.endswith("]"):
                cleaned_response = "[" + cleaned_response[1:]  # Remove any accidental first character
            elif cleaned_response.endswith("}") and cleaned_response.startswith("{"):
                cleaned_response = "[" + cleaned_response + "]"  # Handle single object as an array
            print(f"Cleaned response data: {cleaned_response[:500]}")  # Check the cleaned response before parsing

            # Try parsing the cleaned response
            data_parsed = json.loads(cleaned_response)

            # Check if the parsed data is in the expected format (list of records or an object containing a list)
            if isinstance(data_parsed, dict):
                if "data" in data_parsed:
                    return data_parsed["data"]
                else:
                    print(f"Response is an object but 'data' key not found. Full response: {data_parsed}")
                    return None
            elif isinstance(data_parsed, list):
                return data_parsed
            else:
                print("Response is neither a list nor a dictionary containing 'data'. Full response:")
                print(data_parsed)
                return None

        except ValueError as e:
            print(f"Error parsing JSON response: {e}")
            return None
    else:
        print(f"Error fetching data: {response.status_code}")
        return None

# Step 4: Write data to CSV file
def write_to_csv(data, filename='/Users/ajitpani/Documents/Snowflake_python_code/github/neonblue_project/snowflake_integration_py/data.csv'):
    if data:
        directory = os.path.dirname(filename)
        if not os.path.exists(directory):
            os.makedirs(directory)

        try:
            keys = data[0].keys()  # Extract headers from the first item in the list
            with open(filename, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=keys)
                writer.writeheader()
                writer.writerows(data)
            print(f"Data written to {filename}")
        except Exception as e:
            print(f"Error writing to CSV: {e}")
    else:
        print("No valid data to write to CSV.")

# Main function to execute the workflow
def main(label_prefix="ajit-test1", row_count=1000, start_date="2024-01-01", end_date="2024-01-03"):
    # Generate a new label with a timestamp to avoid collision
    label = f"{label_prefix}-{int(time.time())}"

    print(f"Using label: {label}")

    # Step 1: Initialize session
    session_response = initialize_session(label)
    if not session_response:
        return  # Exit if session initialization fails

    # Step 2: Convert the start_date and end_date to Unix timestamps with nanoseconds
    start_timestamp = date_to_unix_timestamp_with_nanoseconds(start_date)
    end_timestamp = date_to_unix_timestamp_with_nanoseconds(end_date)

    # Step 3: Fetch data and paginate through all available pages
    all_data = []
    page_number = 0

    while True:
        print(f"Fetching data for page {page_number}...")
        data = fetch_data(label, page_number=page_number, row_count=row_count, start_timestamp=start_timestamp, end_timestamp=end_timestamp)

        # Check if the response data is empty (no records returned)
        if data is None or len(data) == 0 :  # If no data, break the loop
            print("No more data available or empty response. Exiting loop.")
            break

        all_data.extend(data)
        page_number += 1

    # Step 4: Write the filtered data to a CSV file
    write_to_csv(all_data)

# Example usage
if __name__ == "__main__":
    main(label_prefix="ajit-test1", row_count=1000, start_date="2024-01-02", end_date="2024-01-03")
