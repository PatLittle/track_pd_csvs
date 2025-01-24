import os
import requests
import pandas as pd

# Function to fetch a CKAN datastore table and convert it to Parquet
def fetch_and_save_as_parquet(api_url, resource_id, output_dir):
    """
    Fetches a CKAN datastore table by resource ID, converts it to Parquet, and saves it.

    Args:
        api_url (str): CKAN base API URL (e.g., "https://open.canada.ca/data/api/3/action/datastore_search")
        resource_id (str): The CKAN datastore resource ID.
        output_dir (str): Directory to save the Parquet file.

    Returns:
        str: Path to the saved Parquet file.
    """
    try:
        records = []
        offset = 0
        limit = 5000

        while True:
            # Fetch data from CKAN API with pagination
            response = requests.get(api_url, params={"resource_id": resource_id, "limit": limit, "offset": offset})
            response.raise_for_status()
            data = response.json()

            if "result" not in data or "records" not in data["result"]:
                raise ValueError(f"No records found for resource_id: {resource_id}")

            batch = data["result"]["records"]
            if not batch:
                break

            records.extend(batch)
            offset += limit

        # Convert records to a DataFrame
        df = pd.DataFrame(records)

        # Ensure the output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Define the Parquet file path
        parquet_file = os.path.join(output_dir, f"{resource_id}.parquet")

        # Save the DataFrame as Parquet
        df.to_parquet(parquet_file, index=False)
        print(f"Saved Parquet file: {parquet_file}")

        return parquet_file

    except Exception as e:
        print(f"Error processing resource_id {resource_id}: {e}")
        return None

# Main script
if __name__ == "__main__":
    # CKAN API base URL
    API_URL = "https://open.canada.ca/data/api/3/action/datastore_search"

    # List of resource IDs
   
    resource_ids_file = "published_resource_ids.txt"

    # Read resource IDs from the file
    with open(resource_ids_file, "r") as file:
        resource_ids = [line.strip() for line in file if line.strip()]

    # Directory to save Parquet files
    output_directory = "./parquet_files"

    # Fetch each resource and save as Parquet
    for resource_id in resource_ids:
        fetch_and_save_as_parquet(API_URL, resource_id, output_directory)
