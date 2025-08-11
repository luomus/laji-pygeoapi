# Caching utility for essential data
import pickle, json
from pathlib import Path
import datetime
import geopandas as gpd
import pandas as pd
import requests, concurrent.futures
import time
import logging

logger = logging.getLogger(__name__)

gpd.options.io_engine = "pyogrio" # Faster way to read data

def load_or_update_cache(config, cache_expiry_days=7):
    """
    Loads essential data (municipals_gdf, municipals_ids, lookup_df, taxon_df, collection_names, all_value_ranges)
    from cache if fresh, otherwise fetches and updates cache. All cache files share the same timestamp.
    """
    cache_dir = Path('pygeoapi/scripts/cache')
    cache_dir.mkdir(exist_ok=True)
    cache_stamp = cache_dir / 'cache.stamp'

    def is_cache_fresh():
        if not cache_stamp.exists():
            return False
        mtime = datetime.datetime.fromtimestamp(cache_stamp.stat().st_mtime)
        return (datetime.datetime.now() - mtime).days < cache_expiry_days

    # Cache file paths
    municipals_gdf_cache = cache_dir / 'municipals_gdf.pkl'
    municipals_ids_cache = cache_dir / 'municipals_ids.json'
    lookup_df_cache = cache_dir / 'lookup_df.pkl'
    taxon_df_cache = cache_dir / 'taxon_df.pkl'
    collection_names_cache = cache_dir / 'collection_names.json'
    all_value_ranges_cache = cache_dir / 'all_value_ranges.json'

    if is_cache_fresh():
        logging.info("Loading data from cache")
        with open(municipals_gdf_cache, 'rb') as f:
            municipals_gdf = pickle.load(f)
        with open(municipals_ids_cache, 'r', encoding='utf-8') as f:
            municipals_ids = json.load(f)
        with open(lookup_df_cache, 'rb') as f:
            lookup_df = pickle.load(f)
        with open(taxon_df_cache, 'rb') as f:
            taxon_df = pickle.load(f)
        with open(collection_names_cache, 'r', encoding='utf-8') as f:
            collection_names = json.load(f)
        with open(all_value_ranges_cache, 'r', encoding='utf-8') as f:
            all_value_ranges = json.load(f)
    else:
        logging.info("Fetching data from API") 
        municipals_gdf = gpd.read_file('pygeoapi/scripts/resources/municipalities.geojson', engine='pyogrio')
        municipals_ids = get_municipality_ids(f"{config['laji_api_url']}areas?type=municipality&lang=fi&access_token={config['access_token']}&pageSize=1000")
        lookup_df = pd.read_csv('pygeoapi/scripts/resources/lookup_table_columns.csv', sep=';', header=0)
        taxon_df = get_taxon_data(f"{config['laji_api_url']}informal-taxon-groups?lang=fi&pageSize=1000&access_token={config['access_token']}")
        collection_names = get_collection_names(f"{config['laji_api_url']}collections?selected=id&lang=fi&pageSize=1500&langFallback=true&access_token={config['access_token']}")
        ranges1 = get_value_ranges(f"{config['laji_api_url']}/metadata/ranges?lang=fi&asLookupObject=true&access_token={config['access_token']}")
        ranges2 = get_enumerations(f"{config['laji_api_url']}/warehouse/enumeration-labels?access_token={config['access_token']}")
        all_value_ranges = ranges1 | ranges2  # type: ignore
        # Save all to cache
        with open(municipals_gdf_cache, 'wb') as f:
            pickle.dump(municipals_gdf, f)
        with open(municipals_ids_cache, 'w', encoding='utf-8') as f:
            json.dump(municipals_ids, f)
        with open(lookup_df_cache, 'wb') as f:
            pickle.dump(lookup_df, f)
        with open(taxon_df_cache, 'wb') as f:
            pickle.dump(taxon_df, f)
        with open(collection_names_cache, 'w', encoding='utf-8') as f:
            json.dump(collection_names, f)
        with open(all_value_ranges_cache, 'w', encoding='utf-8') as f:
            json.dump(all_value_ranges, f)
        cache_stamp.touch()
    return municipals_gdf, municipals_ids, lookup_df, taxon_df, collection_names, all_value_ranges


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
            logging.error(f"Error fetching data from {url}: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
            attempt += 1
    logging.error(f"Failed to retrieve data from {url} after {max_retries} attempts.")
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
        logging.info(f"Total number of occurrences is {total} in {pages} pages")
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

def get_municipality_ids(url):
    """
    Fetch municipality ids and names from the api.laji.fi/areas endpoint

    Parameters:
    url (str): The URL of the areas (type=municipality) API endpoint.

    Returns:
    dict: A dictionary with municipality names as keys and ids as values.
    """
    data = fetch_json_with_retry(url)
    if data:
        results = data['results']
        municipality_ids_dictionary = {}

        for item in results:
            name = item.get('name')
            id_ = item.get('id')
            municipality_ids_dictionary[name] = id_

        return municipality_ids_dictionary