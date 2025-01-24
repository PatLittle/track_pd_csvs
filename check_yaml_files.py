import os
import requests
from bs4 import BeautifulSoup
import yaml

def get_yaml_file_links(url):
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch URL: {response.status_code}")
        exit(1)
    
    soup = BeautifulSoup(response.content, 'html.parser')
    yaml_links = []
    for link in soup.find_all('a', href=True):
        if link['href'].endswith(('.yaml', '.yml')):
            yaml_links.append('https://github.com' + link['href'].replace('/blob/', '/raw/'))
    return yaml_links

def download_file(url, save_path):
    response = requests.get(url)
    if response.status_code == 200:
        with open(save_path, 'wb') as file:
            file.write(response.content)
    else:
        print(f"Failed to download file: {response.status_code}")

def extract_published_resource_ids(file_path):
    published_resource_ids = []
    with open(file_path, 'r') as file:
        try:
            content = yaml.safe_load(file)
            if isinstance(content, dict):
                for key, value in content.items():
                    if key == 'published_resource_id':
                        published_resource_ids.append(value)
                    elif isinstance(value, list):
                        for item in value:
                            if isinstance(item, dict) and 'published_resource_id' in item:
                                published_resource_ids.append(item['published_resource_id'])
        except yaml.YAMLError as exc:
            print(f"Error parsing YAML file {file_path}: {exc}")
    return published_resource_ids

def main(url):
    yaml_links = get_yaml_file_links(url)
    all_published_resource_ids = []
    for yaml_link in yaml_links:
        file_name = os.path.basename(yaml_link)
        download_file(yaml_link, file_name)
        published_resource_ids = extract_published_resource_ids(file_name)
        all_published_resource_ids.extend(published_resource_ids)
        os.remove(file_name)
    
    with open('published_resource_ids.txt', 'w') as output_file:
        for resource_id in all_published_resource_ids:
            output_file.write(f"{resource_id}\n")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python check_yaml_files.py <url>")
        sys.exit(1)
    url = sys.argv[1]
    main(url)