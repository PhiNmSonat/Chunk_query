import requests
import json
import csv
from datetime import datetime, timedelta
import time

BROKER_URL = "https://bi-data.sonatgame.com/druid/v2/sql"

token = 'aHVuZ25kQHNvbmF0LnZuOkh1bmdOVkRBQGtqZjg0amQ2Mzk='
output_csv = "query_results.csv"
query_file = "./base_query.sql"

start_time = datetime(2024, 12, 23, 17, 0, 0)
end_time = datetime(2024, 12, 29, 17, 0, 0)

time_interval = timedelta(days=1) 

headers = {    
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "Authorization": f"Basic {token}",
    "Connection": "keep-alive",
    "Content-Type": "application/json",
    "Origin": "https://bi-data.sonatgame.com",
    "Referer": "https://bi-data.sonatgame.com/unified-console.html",
    "Sec-CH-UA": '"Microsoft Edge";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
}

def load_query(file_path):
    with open(file_path, "r") as file:
        return file.read()

def fetch_data(query):
    try:
        response = requests.post(BROKER_URL, headers=headers, data=json.dumps({"query": query}))
        if response.status_code == 200:
            return response.json()  # Adjust based on API response structure
        else:
            print(f"Error {response.status_code}: {response.text}")
            return []
    except Exception as e:
        print("Failed to connect to the broker:", str(e))
        return []

base_query = load_query(query_file)

with open(output_csv, mode='w', newline='', encoding='utf-8') as csvfile:
    csv_writer = None  # Writer will be initialized dynamically
    current_start_time = start_time

    while current_start_time < end_time:
        current_end_time = min(current_start_time + time_interval, end_time)
        print(f"Fetching data from {current_start_time} to {current_end_time}")
        
        chunk_start_time = time.time()

        # Replace placeholders in the query
        query = base_query.format(
            start_time=current_start_time.isoformat(),
            end_time=current_end_time.isoformat()
        )

        # Fetch data
        data = fetch_data(query)
        if data:
            if not csv_writer:
                # Dynamically set headers from the first result
                column_names = list(data[0].keys()) if data else []
                csv_writer = csv.writer(csvfile)
                csv_writer.writerow(column_names)

            # Write each row dynamically
            for record in data:
                csv_writer.writerow([record.get(column) for column in column_names])

        # End time for the current chunk
        chunk_end_time = time.time()
        chunk_duration = chunk_end_time - chunk_start_time
        print(f"Time taken for this chunk: {chunk_duration:.2f} seconds")
        current_start_time = current_end_time
        time.sleep(2)

print(f"Data saved to {output_csv}")
