import geopandas as gpd
import pandas as pd
import requests, concurrent.futures
import urllib.error
import time

gpd.options.io_engine = "pyogrio" # Faster way to read data

def fetch_json_with_retry(url, max_retries=5, delay=30):
    """
    Fetches JSON data from an API URL with retry logic.
    
    Parameters:
    url (str): The API URL to fetch JSON data from.
    max_retries (int): The maximum number of retry attempts in case of failure.
    delay (int): The delay between retries in seconds.
    
    Returns:
    dict: Parsed JSON data from the API as a dictionary, or None if the request fails.
    """
    attempt = 0
    while attempt < max_retries:
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from {url}: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
            attempt += 1
    print(f"Failed to retrieve data from {url} after {max_retries} attempts.")
    return None

def get_collection_names(url):
    """
    Get collection names in Finnish from the API

    Parameters:
    url (str): The URL of the Collection API endpoint.

    Returns:
    (dictionary): The dictionary containing all collection IDs and their long names
    """

    data = fetch_json_with_retry(url)

    # Extracting collection ids and longNames and storing them in a dictionary
    if data:
        return {item['id']: item['longName'] for item in data['results']}
    return {}

def get_last_page(url):
    """
    Get the last page number from the API response with retry logic.

    Parameters:
    url (str): The URL of the Warehouse API endpoint.

    Returns:
    int: The last page number. Returns None if all retries fail.
    """
    attempt = 0
    max_retries = 3
    delay = 10
    while attempt < max_retries:
        try:
            response = requests.get(url)
            response.raise_for_status()
            api_response = response.json()
            return api_response.get("lastPage", None)
        except (requests.exceptions.RequestException, ValueError) as e:
            print(f"Error retrieving last page from {url}: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
            attempt += 1
    print(f"Failed to retrieve last page from {url} after {max_retries} attempts.")
    return None
       
def download_page(url, page_no):
    """
    Download data from a specific page of the API with retry logic. This is in separate function to speed up multiprocessing.

    Parameters:
    url (str): The URL of the Warehouse API endpoint.
    page_no (int): The page number to download.

    Returns:
    geopandas.GeoDataFrame: The downloaded data as a GeoDataFrame.
    """
    # Load data
    attempt = 0
    max_retries = 5
    delay = 30
    url = url.replace('page=1', f'page={page_no}')
    while attempt < max_retries:
        try:
            gdf = gpd.read_file(url)   
            return gdf 
        except urllib.error.HTTPError as e:
            print(f"HTTP Error {e.code}: {e.reason} for {url}. Retrying in {delay} seconds...")
        except Exception as e:
            print(f"Error downloading page {page_no}: {e}. Retrying in {delay} seconds...")
        time.sleep(delay)
        attempt += 1

    # Return an empty GeoDataFrame in case of too many errors
    print(f"Failed to download data from page {page_no} after {max_retries} attempts.")
    return gpd.GeoDataFrame()

def get_occurrence_data(url, startpage, endpage, multiprocessing=False):
    """
    Retrieve occurrence data from the API.

    Parameters:
    url (str): The URL of the Warehouse API endpoint.
    multiprocessing (bool, optional): Whether to use multiprocessing. Defaults to False.
    startpage (int): First page to retrieve. 
    endpage (int): Last page to retrieve 

    Returns:
    geopandas.GeoDataFrame: The retrieved occurrence data as a GeoDataFrame.
    """    
    
    gdf = gpd.GeoDataFrame()

    if multiprocessing==True or multiprocessing=="True":
        # Use multiprocessing to retrieve page by page. Finally merge all pages into one geodataframe
        with concurrent.futures.ProcessPoolExecutor() as executor:
            futures = [executor.submit(download_page, url, page_no) for page_no in range(startpage, endpage + 1)]
            for future in concurrent.futures.as_completed(futures):
                gdf = pd.concat([gdf, future.result()], ignore_index=True)
    else:
        # Retrieve data page by page without multiprocessing 
        for page_no in range(startpage,endpage+1):
            next_gdf = download_page(url, page_no)
            gdf = pd.concat([gdf, next_gdf], ignore_index=True)

    return gdf

def find_main_taxon(row):
    """
    Find main taxon (taxon with the smallest number)

    Parameters:
    row (list): Pandas dataframe row

    Returns:
    min_value (str): Smallest taxon value from the list
    """
    if type(row) is list:
        numeric_values = [int(value.split('.')[1]) for value in row]
        min_value = 'MVL.' + str(min(numeric_values))
    else:
        min_value = str(row)

    return min_value

def get_value_ranges(url):
    """
    Fetches JSON data from an API URL and returns it as a Python dictionary.

    Parameters:
    url (str): The URL of the metadata API endpoint.

    Returns:
    dict: A dictionary containing the JSON data from the API.
    """
    return fetch_json_with_retry(url)

def get_taxon_data(url):
    """
    Retrieve taxon data from the API. Will be merged to occurrence data later.

    Parameters:
    url (str): The URL of the taxon name API endpoint.

    Returns:
    pandas.DataFrame: The retrieved taxon data.
    """

    # Get the another taxon data
    data = fetch_json_with_retry(url)
    if data:
        json_data_results = data.get('results', [])
        return pd.json_normalize(json_data_results)
    return pd.DataFrame()

def get_enumerations(url):
    """
    Fetches JSON data from an API URL and extracts a dictionary of enumerations 
    with 'enumeration' as keys and 'fi' labels as values.

    Parameters:
    url (str): The URL of the data warehouse enumeration API endpoint.

    Returns:
    dict: A dictionary with enumeration values as keys and 'fi' labels as values.
    """
    json_data = fetch_json_with_retry(url)
    if not json_data:
        print("Error getting enumeration values.")
        return {}

    # Extract "enumeration" keys and "fi" labels
    enumerations = {
        item['enumeration']: item['label']['fi']
        for item in json_data.get('results', [])
    }
    return enumerations
