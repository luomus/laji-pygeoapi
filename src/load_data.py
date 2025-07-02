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

def get_last_page(url, max_retries=5, delay=60):
    """
    Get the last page number from the API response with retry logic.

    Parameters:
    url (str): The URL of the Warehouse API endpoint.

    Returns:
    int: The last page number. Returns None if all retries fail.
    """
    url = url.replace('/list/', '/count/').replace('&geoJSON=true&featureType=ORIGINAL_FEATURE', '')
    api_response = fetch_json_with_retry(url, max_retries=max_retries, delay=delay)
    if api_response:
        total = api_response.get('total')
        pages = total // 10000 
        if total % 10000 != 0:
            pages += 1
        print(f"Total number of occurrences is {total} in {pages} pages")
        return pages
    else:
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
    url = url.replace('page=1', f'page={page_no}')
    data = fetch_json_with_retry(url)
    if data:
        return gpd.GeoDataFrame.from_features(data["features"], crs="EPSG:4326")
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
    int: The estimated number of failed features (if any).
    """    
    failed_features_counter = 0
    gdfs = []

    if multiprocessing in [True, "True"]:
        # Use multiprocessing to retrieve page by page. 
        with concurrent.futures.ProcessPoolExecutor() as executor:
            futures = [executor.submit(download_page, url, page_no) for page_no in range(startpage, endpage + 1)]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                gdfs.append(result)
                if result.empty:
                    failed_features_counter += 10000
    else:
        # Retrieve data page by page without multiprocessing 
        for page_no in range(startpage,endpage+1):
            next_gdf = download_page(url, page_no)
            gdfs.append(next_gdf)
            if next_gdf.empty:
                failed_features_counter += 10000

    # Finally merge all pages into one geodataframe
    gdf = pd.concat(gdfs, ignore_index=True) if gdfs else gpd.GeoDataFrame()
    return gdf, failed_features_counter

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
    json_data = fetch_json_with_retry(url, max_retries=10, delay=60)
    if not json_data:
        raise ValueError("Error getting enumeration values.")

    # Extract "enumeration" keys and "fi" labels
    enumerations = {
        item['enumeration']: item['label']['fi']
        for item in json_data.get('results', [])
    }
    return enumerations
