import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy_utils import database_exists, create_database
import requests, pyogrio, psycopg2, geoalchemy2, os, concurrent.futures

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
        last_page = api_response.get("lastPage")
        return last_page
    except Exception as e:
        print("An error occurred when getting the last page of api results. Perhaps JSON file is invalid. Returning only the first page.")
        return 1
        

def download_page(data_url, page_no, endpage):
    """
    Download data from a specific page of the API. This is in separate function to speed up multiprocessing.

    Parameters:
    data_url (str): The URL of the API endpoint.
    page_no (int): The page number to download.

    Returns:
    geopandas.GeoDataFrame: The downloaded data as a GeoDataFrame.
    """
    # Load data
    data_url = data_url.replace('page=1', f'page={page_no}')
    gdf = gpd.read_file(data_url)
    print(f"Progress: {page_no}/{endpage} pages retrieved")
    
    return gdf

def get_occurrence_data(data_url, multiprocessing=False, startpage = 1, pages="all"):
    """
    Retrieve occurrence data from the API.

    Parameters:
    data_url (str): The URL of the API endpoint.
    multiprocessing (bool, optional): Whether to use multiprocessing. Defaults to False.
    pages (str or int, optional): Number of pages to retrieve. Defaults to "all".

    Returns:
    geopandas.GeoDataFrame: The retrieved occurrence data as a GeoDataFrame.
    """

    if pages == 'all':
        endpage = get_last_page(data_url)
    else:
        endpage = int(pages)
    
    print(f"Retrieving data from page {startpage} to {endpage} of occurrence data from the API...")
    gdf = gpd.GeoDataFrame()

    if multiprocessing==True:
        # Use multiprocessing to retrieve page by page. Finally merge all pages into one geodataframe
        with concurrent.futures.ProcessPoolExecutor() as executor:
            futures = [executor.submit(download_page, data_url, page_no, endpage) for page_no in range(startpage, endpage + 1)]
            for future in concurrent.futures.as_completed(futures):
                gdf = pd.concat([gdf, future.result()], ignore_index=True)


    else:
        # Retrieve data page by page without multiprocessing 
        for page_no in range(startpage,endpage+1):
            next_gdf = download_page(data_url, page_no, endpage)
            gdf = pd.concat([gdf, next_gdf], ignore_index=True)

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
        endpage = get_last_page(taxon_id_url)
    else:
        endpage = int(pages)
        
    print(f"Retrieving {endpage} pages of taxon data from the API...")
    id_df = pd.DataFrame()
    for page_no in range(1, endpage + 1):
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
    df = pd.merge(id_df, name_df, left_on='mainTaxon', right_on='id', suffixes=('MainTaxon', 'TaxonName'))

    # Drop some columns
    df = df.drop(['intellectualRights','vernacularName.fi','vernacularName.en','vernacularName.sv','vernacularName.se','informalTaxonGroups','hasSubGroup'], axis=1)

    return df