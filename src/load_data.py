import geopandas as gpd
import pandas as pd
import requests, concurrent.futures
import urllib.error
import time

gpd.options.io_engine = "pyogrio" # Faster way to read data

def get_collection_names(api_url):
    """
    Get collection names in Finnish from the API

    Parameters:
    api_url (str): The URL of the API endpoint.

    Returns:
    ids_and_names (dictionary): The dictionary containing all collection IDs and their long names
    """
    # Fetching the JSON data from the API
    response = requests.get(api_url)
    data = response.json()

    # Extracting collection ids and longNames and storing them in a dictionary
    ids_and_names = {item['id']: item['longName'] for item in data['results']}

    return ids_and_names

def get_last_page(data_url):
    """
    Get the last page number from the API response with retry logic.

    Parameters:
    data_url (str): The URL of the API endpoint.

    Returns:
    int: The last page number. Returns None if all retries fail.
    """
    attempt = 0
    max_retries = 3
    delay = 10
    while attempt < max_retries:
        try:
            response = requests.get(data_url)
            
            # Check if the request was successful
            if response.status_code != 200:
                print(f"Request failed with status code {response.status_code}")
                time.sleep(delay)
                attempt += 1
                continue

            # Try parsing the response as JSON
            try:
                api_response = response.json()
            except ValueError:  # Catch JSON decoding errors
                print("Failed to parse JSON response")
                print(f"Response text: {response.text}")
                time.sleep(delay)
                attempt += 1
                continue
        
            # Get the last page from the API response
            last_page = api_response.get("lastPage")
            if last_page is None:
                print("No 'lastPage' key found in the response")
            return last_page
        
        except requests.exceptions.RequestException as e:
            print(f"Error occurred during the request: {e}")
            time.sleep(delay)
            attempt += 1

    # If all attempts fail, return None
    print(f"All {max_retries} attempts failed.")
    return None
       
def download_page(data_url, page_no):
    """
    Download data from a specific page of the API with retry logic. This is in separate function to speed up multiprocessing.

    Parameters:
    data_url (str): The URL of the API endpoint.
    page_no (int): The page number to download.

    Returns:
    geopandas.GeoDataFrame: The downloaded data as a GeoDataFrame.
    """
    # Load data
    attempt = 0
    max_retries = 3
    delay = 10
    data_url = data_url.replace('page=1', f'page={page_no}')
    while attempt < max_retries:
        try:
            gdf = gpd.read_file(data_url)   
            return gdf 
        except urllib.error.HTTPError as e:
            print(f"HTTP Error {e.code}: {e.reason}. Could not access {data_url}.")
            print(f"Retrying in {delay} seconds...")
            time.sleep(delay)
            attempt += 1
            continue
        except Exception as e:
            # Catch any other exceptions
            print(f"An error occurred: {e}")
            print(f"Retrying in {delay} seconds...")
            time.sleep(delay)
            attempt += 1
            continue

    # Return an empty GeoDataFrame in case of too many errors
    print(f"Failed to download data from page {page_no} after {max_retries} attempts.")
    return gpd.GeoDataFrame()

def get_occurrence_data(data_url, startpage, endpage, multiprocessing=False):
    """
    Retrieve occurrence data from the API.

    Parameters:
    data_url (str): The URL of the API endpoint.
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
            futures = [executor.submit(download_page, data_url, page_no) for page_no in range(startpage, endpage + 1)]
            for future in concurrent.futures.as_completed(futures):
                gdf = pd.concat([gdf, future.result()], ignore_index=True)
    else:
        # Retrieve data page by page without multiprocessing 
        for page_no in range(startpage,endpage+1):
            next_gdf = download_page(data_url, page_no)
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

def get_taxon_data(taxon_name_url):
    """
    Retrieve taxon data from the API. Will be merged to occurrence data later.

    Parameters:
    taxon_name_url (str): The URL of the taxon name API endpoint.

    Returns:
    pandas.DataFrame: The retrieved taxon data.
    """

    # Get the another taxon data
    response = requests.get(taxon_name_url)
    json_data_results = response.json().get('results', [])
    df = pd.json_normalize(json_data_results)

    return df