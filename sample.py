import os
from google.cloud import bigquery
import requests
import csv

# Define your Polygon.io API key
api_key = "SyJY_Mq3g91e8BZIjCGGrFuIf1NW5738"

# Define the API endpoint URL
api_url = "https://api.polygon.io/v2/aggs/ticker/AAPL/range/5/day/2023-01-01/2023-01-05"

# Send a GET request to the API endpoint with the API key
response = requests.get(api_url, params={"apiKey": api_key})

# Check if the request was successful (HTTP status code 200)
if response.status_code == 200:
    print('SUCCESS! Status Code 200')
    # Parse the JSON response content
    data = response.json()

    # Create the 'output' folder if it doesn't exist
    output_folder = "output"
    os.makedirs(output_folder, exist_ok=True)

    # Define the CSV file path
    csv_file = os.path.join(output_folder, "stock_data.csv")

    # Extract the data points from the JSON response
    print(data)
    data_points = data.get("results")

    if data_points:
        # Open the CSV file for writing
        with open(csv_file, "w", newline="") as csvfile:
            # Create a CSV writer
            csv_writer = csv.writer(csvfile)

            # Write the header row to the CSV file
            csv_writer.writerow(["Timestamp", "Open", "High", "Low", "Close", "Volume"])

            # Write data to the CSV file
            for point in data_points:
                csv_writer.writerow([
                    point["t"],
                    point["o"],
                    point["h"],
                    point["l"],
                    point["c"],
                    point["v"]
                ])

        print(f"--> Data has been saved to {csv_file}")

        # Upload the CSV file to Google BigQuery
        project_id = "stock-data-401620"
        dataset_id = "stock_data"
        table_id = "APPL"
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        client = bigquery.Client(project=project_id)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
        )

        with open(csv_file, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

        job.result()
        print(f"--> Data has been uploaded to BigQuery table {table_ref}")

    else:
        print("--> No data points found in the response.")
else:
    # If the request was not successful, print an error message
    print(f"--> Failed to retrieve data. Status code: {response.status_code}")