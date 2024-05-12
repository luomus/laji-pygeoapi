import geopandas as gpd
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy_utils import database_exists, create_database
import requests, pyogrio, psycopg2, geoalchemy2, os, concurrent.futures
import edit_config, edit_db, process_data

def get_last_page(data_url):
    """
    Get the last page number from the API response.

    Parameters:
    data_url (str): The URL of the API endpoint.

    Returns:
    int: The last page number.
    """
    try:
        response = requests.get(data_url)
        api_response = response.json()
        return(api_response.get("lastPage"))
    except Exception as e:
        print("An error occurred:", e)

def download_page(data_url, page_no):
    """
    Download data from a specific page of the API. This is in separate function to speed up multiprocessing.

    Parameters:
    data_url (str): The URL of the API endpoint.
    page_no (int): The page number to download.

    Returns:
    geopandas.GeoDataFrame: The downloaded data as a GeoDataFrame.
    """
    data_url = data_url.replace('page=1', f'page={page_no}')
    gdf = gpd.read_file(data_url)
    return gdf

def get_occurrence_data(data_url, multiprocessing=True, pages="all"):
    """
    Retrieve occurrence data from the API.

    Parameters:
    data_url (str): The URL of the API endpoint.
    multiprocessing (bool, optional): Whether to use multiprocessing. Defaults to True.
    pages (str or int, optional): Number of pages to retrieve. Defaults to "all".

    Returns:
    geopandas.GeoDataFrame: The retrieved occurrence data as a GeoDataFrame.
    """

    if pages == 'all':
        last_page = get_last_page(data_url)
    else:
        last_page = int(pages)
    
    print(f"Retrieving {last_page} pages of occurrence data from the API...")
    gdf = gpd.GeoDataFrame()
    if multiprocessing==True:
        # Use multiprocessing to retrieve page by page. Finally merge all pages into one geodataframe
        with concurrent.futures.ProcessPoolExecutor() as executor:
            futures = [executor.submit(download_page, data_url, page_no) for page_no in range(1, last_page + 1)]
            progress_bar = tqdm(total=last_page)
            for future in tqdm(concurrent.futures.as_completed(futures)):
                progress_bar.update(1)
                gdf = pd.concat([gdf, future.result()], ignore_index=True)
            progress_bar.close()

    else:
        # Retrieve data page by page without multiprocessing 
        for page_no in tqdm(range(1,last_page+1)):
            next_gdf = download_page(data_url, page_no)
            print("mergind pages...")
            gdf = pd.concat([gdf, next_gdf])

    return gdf

def get_taxon_data(taxon_id_url, taxon_name_url, pages='all'):
    """
    Retrieve taxon data from the API. Will be merged to occurrence data later.

    Parameters:
    taxon_id_url (str): The URL of the taxon ID API endpoint.
    taxon_name_url (str): The URL of the taxon name API endpoint.
    pages (str or int, optional): Number of pages to retrieve. Defaults to "all".

    Returns:
    pandas.DataFrame: The retrieved taxon data.
    """
    if pages == 'all':
        last_page = get_last_page(taxon_id_url)
    else:
        last_page = int(pages)
        
    print(f"Retrieving {last_page} pages of taxon data from the API...")
    id_df = pd.DataFrame()
    for page_no in tqdm(range(1, last_page + 1)):
        next_page = taxon_id_url.replace('page=1', f'page={page_no}')
        response = requests.get(next_page)
        if response.status_code == 200:
            json_data_results = response.json().get('results', [])
            next_df = pd.json_normalize(json_data_results)
            id_df = pd.concat([id_df, next_df], ignore_index=True)
        else:
            print(f"Failed to fetch data from page {page_no}. Status code: {response.status_code}")

    def find_main_taxon(row):
        # Find main taxon from the taxon group list
        if type(row) is list:
            numeric_values = [int(value.split('.')[1]) for value in row]
            min_value = 'MVL.' + str(min(numeric_values))
        else:
            min_value = str(row)

        return min_value

    # Apply the find_main_taxon function to each row and store the main taxon in a new column
    id_df['mainTaxon'] = id_df['informalTaxonGroups'].apply(find_main_taxon)

    # Get the another taxon data
    response = requests.get(taxon_name_url)
    json_data_results = response.json().get('results', [])
    name_df = pd.json_normalize(json_data_results)

    # Join both taxon data sets together
    df = pd.merge(id_df, name_df, left_on='mainTaxon', right_on='id')

    return df