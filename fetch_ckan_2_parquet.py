import os
import requests
import pandas as pd
import concurrent.futures
from io import BytesIO

# Function to fetch a CKAN datastore dump and convert it to Parquet
def fetch_and_save_as_parquet(api_url, resource_id, output_dir):
    """
    Fetches a CKAN datastore table dump by resource ID, converts it to Parquet, and saves it.

    Args:
        api_url (str): CKAN base API URL (e.g., "https://open.canada.ca/data/datastore/dump/")
        resource_id (str): The CKAN datastore resource ID.
        output_dir (str): Directory to save the Parquet file.

    Returns:
        str: Path to the saved Parquet file, or None if failed.
    """
    try:
        # Construct the datastore dump URL
        dump_url = f"{api_url}{resource_id}"

        # Make the request to download the dump
        response = requests.get(dump_url, stream=True, timeout=30)
        response.raise_for_status()

        # Load CSV data into a DataFrame directly from the response
        df = pd.read_csv(BytesIO(response.content), low_memory=False)

        # Ensure the output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Define the Parquet file path
        parquet_file = os.path.join(output_dir, f"{resource_id}.parquet")

        # Save the DataFrame as Parquet with compression
        df.to_parquet(parquet_file, index=False, compression="snappy")
        print(f"Saved Parquet file: {parquet_file}")

        return parquet_file

    except Exception as e:
        print(f"Error processing resource_id {resource_id}: {e}")
        return None

# Parallelized function to process resources
def process_resources(api_url, resource_ids, output_dir, max_workers=5):
    """
    Process multiple CKAN datastore resources in parallel.

    Args:
        api_url (str): CKAN base API URL.
        resource_ids (list): List of resource IDs.
        output_dir (str): Directory to save Parquet files.
        max_workers (int): Number of parallel workers.

    Returns:
        list: List of processed resource IDs and their Parquet paths.
    """
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_and_save_as_parquet, api_url, resource_id, output_dir): resource_id
            for resource_id in resource_ids
        }
        for future in concurrent.futures.as_completed(futures):
            resource_id = futures[future]
            try:
                result = future.result()
                if result:
                    results.append((resource_id, result))
            except Exception as e:
                print(f"Error processing resource_id {resource_id}: {e}")
    return results

# Main script
if __name__ == "__main__":
    # CKAN API base URL
    API_URL = "https://open.canada.ca/data/datastore/dump/"

    # Path to the file containing resource IDs
    resource_ids_file = "published_resource_ids.txt"

    # Read resource IDs from the file
    with open(resource_ids_file, "r") as file:
        resource_ids = [line.strip() for line in file if line.strip()]

    # Directory to save Parquet files
    output_directory = "./parquet_files"

    # Fetch resources in parallel
    process_resources(API_URL, resource_ids, output_directory, max_workers=10)
