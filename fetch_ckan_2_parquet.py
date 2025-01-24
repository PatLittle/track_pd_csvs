import os
import requests
import pandas as pd

# Function to fetch a CKAN datastore dump and convert it to Parquet
def fetch_and_save_as_parquet(api_url, resource_id, output_dir):
    """
    Fetches a CKAN datastore table dump by resource ID, converts it to Parquet, and saves it.

    Args:
        api_url (str): CKAN base API URL (e.g., "https://open.canada.ca/data/api/3/action/datastore_dump")
        resource_id (str): The CKAN datastore resource ID.
        output_dir (str): Directory to save the Parquet file.

    Returns:
        str: Path to the saved Parquet file.
    """
    try:
        # Construct the datastore dump URL
        dump_url = f"{api_url}?resource_id={resource_id}"

        # Make the request to download the dump
        response = requests.get(dump_url, stream=True)
        response.raise_for_status()

        # Ensure the output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Temporary file to save the dump
        csv_file = os.path.join(output_dir, f"{resource_id}.csv")
        with open(csv_file, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk)

        # Read the CSV file into a DataFrame
        df = pd.read_csv(csv_file)

        # Define the Parquet file path
        parquet_file = os.path.join(output_dir, f"{resource_id}.parquet")

        # Save the DataFrame as Parquet
        df.to_parquet(parquet_file, index=False)
        print(f"Saved Parquet file: {parquet_file}")

        # Remove the temporary CSV file
        os.remove(csv_file)

        return parquet_file

    except Exception as e:
        print(f"Error processing resource_id {resource_id}: {e}")
        return None

# Main script
if __name__ == "__main__":
    # CKAN API base URL
    API_URL = "https://open.canada.ca/data/api/3/action/datastore_dump"

    # Path to the file containing resource IDs
    resource_ids_file = "published_resource_ids.txt"

    # Read resource IDs from the file
    with open(resource_ids_file, "r") as file:
        resource_ids = [line.strip() for line in file if line.strip()]

    # Directory to save Parquet files
    output_directory = "./parquet_files"

    # Fetch each resource and save as Parquet
    for resource_id in resource_ids:
        fetch_and_save_as_parquet(API_URL, resource_id, output_directory)
