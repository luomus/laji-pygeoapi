# Caching utility for essential data
import geopandas as gpd
import pandas as pd
import requests, concurrent.futures
import time
import logging
import functools

logger = logging.getLogger(__name__)

gpd.options.io_engine = "pyogrio" # Faster way to read data

# Simple in-memory cache
_cache = {}
_cache_timeout = 86400  # 1 day in seconds
_cache_timestamps = {}

def _is_cache_valid(key):
    """Check if cached data is still valid"""
    if key not in _cache_timestamps:
        return False
    return (time.time() - _cache_timestamps[key]) < _cache_timeout

def _get_api_headers(access_token):
    """Build headers for API requests"""
    return {
        'Authorization': f'Bearer {access_token}',
        'Api-Version': '1'
    }

def load_or_update_cache(config):
    """
    Loads essential data (municipality_ely_mappings, municipals_ids, lookup_df, taxon_df, collection_names, all_value_ranges, municipality_elinvoima_mappings) from the cache or the API.
    """
    cache_key = f"helper_data_{config.get('laji_api_url', '')}"
    
    # Check if we have valid cached data
    if cache_key in _cache and _is_cache_valid(cache_key):
        logger.debug("Using cached helper data")
        return _cache[cache_key]

    logger.debug("Fetching data from API")
    base_url = config['laji_api_url']
    headers = _get_api_headers(config['access_token'])

    municipality_df = pd.read_json('scripts/resources/municipality_ely_mappings.json').set_index('Municipal_Name')
    municipality_ely_mappings = municipality_df['ELY_Area_Name']
    municipality_elinvoima_mappings = municipality_df['Elinvoimakeskus_Name']

    municipals_ids = get_municipality_ids(f"{base_url}areas", {'areaType': 'ML.municipality', 'lang': 'fi', 'pageSize': 1000}, headers)
    lookup_df = pd.read_csv('scripts/resources/lookup_table_columns.csv', sep=';', header=0)
    taxon_df = get_taxon_data(f"{base_url}informal-taxon-groups", {'lang': 'fi', 'pageSize': 1000}, headers)
    collection_names = get_collection_names(f"{base_url}collections", {'selected': 'id', 'lang': 'fi', 'pageSize': 1500, 'langFallback': 'true'}, headers)
    ranges1 = get_value_ranges(f"{base_url}metadata/alts", {'lang': 'fi'}, headers)
    ranges2 = get_enumerations(f"{base_url}warehouse/enumeration-labels", {}, headers)
    all_value_ranges = ranges1 | ranges2  # type: ignore

    result = municipality_ely_mappings, municipals_ids, lookup_df, taxon_df, collection_names, all_value_ranges, municipality_elinvoima_mappings

    # Cache the result
    _cache[cache_key] = result
    _cache_timestamps[cache_key] = time.time()

    return result

@functools.cache
def get_filter_values(filter_name, access_token, base_url=None):
    """
    Fetch filter values from the API and return them as a dictionary with names as keys and Finnish labels as values.
    
    Parameters:
    filter_name (str): The name of the filter to retrieve values for.
    access_token (str): The access token for API authentication.
    base_url (str): The base URL for the API. If None, defaults to https://api.laji.fi/
    
    Returns:
    dict: A dictionary with enumeration names as keys and Finnish labels as values.
    """
    if base_url is None:
        base_url = 'https://api.laji.fi/'
    url = f'{base_url}warehouse/filters/{filter_name}'
    headers = _get_api_headers(access_token)
    data = fetch_json_with_retry(url, headers=headers)
    if data:
        enumerations = data.get('enumerations', [])
        return {
            item['label']['fi']: item['name']
            for item in enumerations
            if item.get('label') and item['label'].get('fi')
        }
    logger.error(f"Failed to retrieve values for {filter_name}")
    return {}

def fetch_json_with_retry(url, params=None, headers=None, max_retries=5, delay=30):
    """
    Fetches JSON data from an API URL with retry logic.
    
    Parameters:
    url (str): The base API URL to fetch JSON data from.
    params (dict): Query parameters to include in the request.
    headers (dict): Headers to include in the request.
    max_retries (int): The maximum number of retry attempts in case of failure.
    delay (int): The delay between retries in seconds.
    
    Returns:
    dict: Parsed JSON data from the API as a dictionary, or None if the request fails.
    """
    attempt = 0
    while attempt < max_retries:
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from {url}: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
            attempt += 1
    logger.error(f"Failed to retrieve data from {url} after {max_retries} attempts.")
    return None

def get_collection_names(url, params, headers):
    """
    Get collection names in Finnish from the API

    Parameters:
    url (str): The base URL of the Collection API endpoint.
    params (dict): Query parameters for the request.
    headers (dict): Headers for the request.

    Returns:
    (dictionary): The dictionary containing all collection IDs and their long names
    """

    data = fetch_json_with_retry(url, params=params, headers=headers)

    # Extracting collection ids and longNames and storing them in a dictionary
    if data:
        return {item['id']: item['longName'] for item in data['results']}
    return {}

def get_pages(pages_env: str, occurrence_url: str, params: dict, headers: dict, page_size: int) -> int:
    """Resolve number of pages to process based on pages_env flag.

    pages_env values:
      - 'all' or 'latest': query API for total pages
      - '0': special case handled earlier (drop all tables)
      - numeric string: explicit fixed number of pages
    """
    if pages_env in ("all", "latest"):
        pages = get_last_page(occurrence_url, params, headers, int(page_size))
        return pages or 0
    if pages_env.isdigit():
        return int(pages_env)
    raise ValueError(f"Unsupported PAGES value: {pages_env}")

def get_last_page(url, params, headers, page_size, max_retries=5, delay=60):
    """
    Get the last page number from the API response with retry logic.

    Parameters:
    url (str): The base URL of the Warehouse API endpoint.
    params (dict): Query parameters for the request.
    headers (dict): Headers for the request.
    page_size (int): The number of items per page.

    Returns:
    int: The last page number. Returns None if all retries fail.
    """
    count_url = url.replace('/list/', '/count/')
    # Remove geoJSON parameters for count endpoint
    count_params = {k: v for k, v in params.items() if k not in ['geoJSON', 'featureType']}
    
    api_response = fetch_json_with_retry(count_url, params=count_params, headers=headers, max_retries=max_retries, delay=delay)
    if api_response:
        total = api_response.get('total')
        pages = total // page_size
        if total % page_size != 0:
            pages += 1
        logger.info(f"Total number of occurrences is {total} in {pages} pages")
        return pages
    else:
        return None

def download_page(url, params, headers, page_no):
    """
    Download data from a specific page of the API with retry logic. This is in separate function to speed up multiprocessing.

    Parameters:
    url (str): The base URL of the Warehouse API endpoint.
    params (dict): Query parameters for the request.
    headers (dict): Headers for the request.
    page_no (int): The page number to download.

    Returns:
    geopandas.GeoDataFrame: The downloaded data as a GeoDataFrame.
    """
    page_params = params.copy()
    page_params['page'] = page_no
    data = fetch_json_with_retry(url, params=page_params, headers=headers)
    if data:
        return gpd.GeoDataFrame.from_features(data["features"], crs="EPSG:4326")
    return gpd.GeoDataFrame()

def get_occurrence_data(url, params, headers, startpage, endpage, multiprocessing=False):
    """
    Retrieve occurrence data from the API.

    Parameters:
    url (str): The base URL of the Warehouse API endpoint.
    params (dict): Query parameters for the request.
    headers (dict): Headers for the request.
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
            futures = [executor.submit(download_page, url, params, headers, page_no) for page_no in range(startpage, endpage + 1)]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                gdfs.append(result)
                if result.empty:
                    failed_features_counter += 10000
    else:
        # Retrieve data page by page without multiprocessing 
        for page_no in range(startpage,endpage+1):
            next_gdf = download_page(url, params, headers, page_no)
            gdfs.append(next_gdf)
            if next_gdf.empty:
                failed_features_counter += 10000

    # Finally merge all pages into one geodataframe
    gdf = pd.concat(gdfs, ignore_index=True) if gdfs else gpd.GeoDataFrame()
    return gdf, failed_features_counter

def get_value_ranges(url, params, headers):
    """
    Return a simple flat dict of id:value pairs from a nested JSON structure.
    The JSON is expected to be a dict where each value is a list of dicts with 'id' and 'value' keys.
    """
    json_data = fetch_json_with_retry(url, params=params, headers=headers)
    if not json_data:
        raise ValueError("Error getting value ranges.")

    # Flatten nested structure: {key: [{id:..., value:...}, ...], ...}
    range_values = {}
    for key, items in json_data.items():
        for item in items:
            if isinstance(item, dict) and item.get('id') and item.get('value'):
                range_values[item['id']] = item['value']
    return range_values

def get_taxon_data(url, params, headers):
    """
    Retrieve taxon data from the API. Will be merged to occurrence data later.

    Parameters:
    url (str): The base URL of the taxon name API endpoint.
    params (dict): Query parameters for the request.
    headers (dict): Headers for the request.

    Returns:
    pandas.DataFrame: The retrieved taxon data.
    """

    # Get the another taxon data
    data = fetch_json_with_retry(url, params=params, headers=headers)
    if data:
        json_data_results = data.get('results', [])
        return pd.json_normalize(json_data_results)
    return pd.DataFrame()

def get_enumerations(url, params, headers):
    """
    Fetches JSON data from an API URL and extracts a dictionary of enumerations 
    with 'enumeration' as keys and 'fi' labels as values.

    Parameters:
    url (str): The base URL of the data warehouse enumeration API endpoint.
    params (dict): Query parameters for the request.
    headers (dict): Headers for the request.

    Returns:
    dict: A dictionary with enumeration values as keys and 'fi' labels as values.
    """
    json_data = fetch_json_with_retry(url, params=params, headers=headers, max_retries=10, delay=60)
    if not json_data:
        raise ValueError("Error getting enumeration values.")

    # Extract "enumeration" keys and "fi" labels
    enumerations = {
        item['enumeration']: item['label']['fi']
        for item in json_data.get('results', [])
        if item.get('label') and item['label'].get('fi')
    }
    return enumerations

def get_municipality_ids(url, params, headers):
    """
    Fetch municipality ids and names from the api.laji.fi/areas endpoint

    Parameters:
    url (str): The base URL of the areas (type=municipality) API endpoint.
    params (dict): Query parameters for the request.
    headers (dict): Headers for the request.

    Returns:
    dict: A dictionary with municipality names as keys and ids as values.
    """
    data = fetch_json_with_retry(url, params=params, headers=headers)
    if data:
        results = data['results']
        municipality_ids_dictionary = {}

        for item in results:
            name = item.get('name')
            id_ = item.get('id')
            municipality_ids_dictionary[name] = id_

        return municipality_ids_dictionary