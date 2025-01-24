import os
import yaml
import requests

def extract_published_resource_ids(url):
    published_resource_ids = []
    response = requests.get(url)
    if response.status_code == 200:
        for line in response.text.splitlines():
            if line.strip().startswith("published_resource_id:"):
                published_resource_ids.append(line.split(":")[1].strip())
    return published_resource_ids

def save_published_resource_ids_to_file(published_resource_ids, output_file):
    with open(output_file, 'w') as f:
        for resource_id in published_resource_ids:
            f.write(f"{resource_id}\n")

if __name__ == "__main__":
    url = "https://raw.githubusercontent.com/open-data/ckanext-canada/master/ckanext/canada/tables/"
    output_file = "published_resource_ids.txt"
    published_resource_ids = extract_published_resource_ids(url)
    save_published_resource_ids_to_file(published_resource_ids, output_file)
