import requests
import csv
import json
import time
import os
from datetime import datetime

# Convert the date to Unix timestamp with nanoseconds
def date_to_unix_timestamp_with_nanoseconds(date_string):
    dt_object = datetime.strptime(date_string, "%Y-%m-%d")
    timestamp_seconds = int(dt_object.timestamp())  # Convert to seconds
    timestamp_nanoseconds = timestamp_seconds * 1_000_000_000  # Convert to nanoseconds
    return timestamp_nanoseconds

# Initialize the API session with a label
def initialize_session(label):
    url = "https://neonblue--nblearn-hiring-init.modal.run"
    headers = {"Content-Type": "application/json"}
    data = {"label": label}

    response = requests.post(url, json=data, headers=headers)
    if response.status_code == 200:
        print(f"Session initialized with label: {label}")
        return response.json()
    else:
        print(f"Error initializing session: {response.status_code}")
        print(response.text)
        return None

# Fetch data from the API with filtering for created_at field (date range)
def fetch_data(label, page_number=0, row_count=1000, start_timestamp=None, end_timestamp=None):
    url = "https://neonblue--nblearn-hiring-read.modal.run"
    headers = {"Content-Type": "application/json"}

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
    print(f"Response Content (first 500 chars): {response.text[:500]}")  # Debugging: print first 500 characters

    if response.status_code == 200:
        try:
            # Clean the response string, remove unwanted characters like extra quotes or escape sequences
            raw_response = response.text.strip()

            # If the response contains extra quotes or escape sequences, clean it
            if raw_response.startswith('"') and raw_response.endswith('"'):
                raw_response = raw_response[1:-1]  # Remove surrounding quotes if present

            print(f"Cleaned raw response: {raw_response[:500]}")  # Debugging: check the cleaned response

            # Now parse the cleaned response as JSON
            data_parsed = json.loads(raw_response)

            # Extract the "data" field, which contains the actual JSON data in string format
            if "data" in data_parsed:
                # Parse the "data" field which contains string-encoded JSON
                data_in_data_field = json.loads(data_parsed["data"])
                return data_in_data_field
            else:
                print("Error: 'data' field not found in response.")
                return None
        except ValueError as e:
            print(f"Error parsing JSON response: {e}")
            return None
    else:
        print(f"Error fetching data: {response.status_code}")
        return None

# Write data to CSV file
def write_to_csv(data, filename='./data.csv'):  # Saving in the current directory
    if data:
        print(f"Data received for CSV writing: {type(data)}")  # Print the type of data (for debugging)

        # Check the first item in the list to see its structure
        if isinstance(data, list):
            print(f"First item in data: {data[0]}")  # Show the first record to understand its structure

        # Check if the data is a list of dictionaries
        if isinstance(data, list) and isinstance(data[0], dict):
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
            print("Error: Data is not a list of dictionaries.")
            # Additional debugging: Check what the data actually looks like
            print("Data format issue - Inspecting data:")
            print(data[:5])  # Print the first 5 records to inspect their structure
    else:
        print("No valid data to write to CSV.")

# Main function to execute the workflow
def main(label_prefix="ajit-test1", row_count=1000, start_date="2024-01-01", end_date="2024-01-03"):
    # Generate a new label with a timestamp to avoid collision
    label = f"{label_prefix}-{int(time.time())}"

    print(f"Using label: {label}")

    # Initialize session
    session_response = initialize_session(label)
    if not session_response:
        return  # Exit if session initialization fails

    # Convert the start_date and end_date to Unix timestamps with nanoseconds
    start_timestamp = date_to_unix_timestamp_with_nanoseconds(start_date)
    end_timestamp = date_to_unix_timestamp_with_nanoseconds(end_date)

    # Fetch data and paginate through all available pages
    all_data = []
    page_number = 0

    while True:
        print(f"Fetching data for page {page_number}...")
        data = fetch_data(label, page_number=page_number, row_count=row_count, start_timestamp=start_timestamp, end_timestamp=end_timestamp)

        if not data:  # No data returned, exit loop
            print("No more data available or empty response. Exiting loop.")
            break

        all_data.extend(data)
        page_number += 1

    # Write the filtered data to a CSV file
    write_to_csv(all_data)

# Example usage
if __name__ == "__main__":
    main(label_prefix="ajit-test1", row_count=1000, start_date="2024-01-01", end_date="2024-01-03")
