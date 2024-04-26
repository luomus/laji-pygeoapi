import geopandas as gpd
from sqlalchemy import create_engine, text
from sqlalchemy_utils import database_exists, create_database
import re
import pandas as pd
from tqdm import tqdm
import requests
import pyogrio, psycopg2, geoalchemy2
import os
from dotenv import load_dotenv
import concurrent.futures

pd.options.mode.copy_on_write = True
gpd.options.io_engine = "pyogrio" # Faster way to read data

print("Start")
# Path to the GeoJSON file
occurrence_url = r'https://laji.fi/api/warehouse/query/unit/list?administrativeStatusId=MX.finlex160_1997_appendix4_2021,MX.finlex160_1997_appendix4_specialInterest_2021,MX.finlex160_1997_appendix2a,MX.finlex160_1997_appendix2b,MX.finlex160_1997_appendix3a,MX.finlex160_1997_appendix3b,MX.finlex160_1997_appendix3c,MX.finlex160_1997_largeBirdsOfPrey,MX.habitatsDirectiveAnnexII,MX.habitatsDirectiveAnnexIV,MX.birdsDirectiveStatusAppendix1,MX.birdsDirectiveStatusMigratoryBirds&redListStatusId=MX.iucnCR,MX.iucnEN,MX.iucnVU,MX.iucnNT&countryId=ML.206&time=1990-01-01/&aggregateBy=gathering.conversions.wgs84Grid05.lat,gathering.conversions.wgs84Grid1.lon&onlyCount=false&individualCountMin=0&coordinateAccuracyMax=1000&page=1&pageSize=10000&taxonAdminFiltersOperator=OR&collectionAndRecordQuality=PROFESSIONAL:EXPERT_VERIFIED,COMMUNITY_VERIFIED,NEUTRAL,UNCERTAIN;HOBBYIST:EXPERT_VERIFIED,COMMUNITY_VERIFIED,NEUTRAL;AMATEUR:EXPERT_VERIFIED,COMMUNITY_VERIFIED;&geoJSON=true&featureType=ORIGINAL_FEATURE'
#occurrence_url = r'test_data\10000_virva_data.json'
taxon_id_url= r'https://laji.fi/api/taxa/MX.37600/species?onlyFinnish=true&selectedFields=id,vernacularName,scientificName,informalTaxonGroups&lang=multi&page=1&pageSize=1000&sortOrder=taxonomic'
taxon_name_url = r'https://laji.fi/api/informal-taxon-groups?pageSize=1000'
#taxon_file = r'scripts\taxon-export.tsv'
template_resource = r'scripts/template_resource.txt'
pygeoapi_config = r'pygeoapi-config.yml'
lookup_table = r'scripts/lookup_table_columns.csv'

# Database connection parameters from the secret .env file
load_dotenv()
db_params = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER') ,
    'password': os.getenv('POSTGRES_PASSWORD') ,
    'host': 'localhost',
    'port': '5433'
}

def get_last_page(data_url):
    # Get the last page number from API
    try:
        response = requests.get(data_url)
        api_response = response.json()
        return(api_response.get("lastPage"))
    except Exception as e:
        print("An error occurred:", e)

def clean_table_name(group_name):
    # Function to clean and return a table name
    if group_name is None or group_name =='nan' or group_name == '':
        return 'unclassified'
    
    # Remove non-alphanumeric characters and white spaces
    cleaned_name = re.sub(r'[^\w\s]', '', str(group_name)).replace(' ', '_')   

    # Shorten the name
    if len(cleaned_name) > 40:
        cleaned_name = cleaned_name[:40]

    return f'{cleaned_name}'

def column_names_as_dwc(gdf, lookup_table):
    # Load the lookup table CSV into a DataFrame
    lookup_df = pd.read_csv(lookup_table, sep=';', header=0)

    # Map all column names according to the Darwin Core names in the lookup table
    column_mapping = {}
    for _, row in lookup_df.iterrows():
        column_mapping[row['finbif_api_var']] = row['dwc']

    # Rename columns based on the mapping
    gdf.rename(columns=column_mapping, inplace=True)
    return gdf

def get_bbox(sub_gdf):
    # Return bounding box for geometries
    minx, miny, maxx, maxy = sub_gdf.geometry.total_bounds
    return [minx, miny, maxx, maxy]

def get_min_and_max_dates(sub_gdf):
    dates = sub_gdf['eventDateTimeDisplay']
    # Convert the 'formatted_date_time' column to pandas datetime format
    try:
        dates = pd.to_datetime(dates.str.split(' ', expand=True).iloc[:, 0] + 'T00:00:00Z')
    except:
        dates = pd.to_datetime(dates + 'T00:00:00Z')
    
    # Filter out NaT (Not a Time) values
    dates_without_na = dates.dropna()

    # Get the minimum and maximum dates in RFC3339 format
    if len(dates_without_na) > 0:
        start_date = str(dates_without_na.min().strftime('%Y-%m-%dT%H:%M:%SZ'))
        end_date = str(dates_without_na.max().strftime('%Y-%m-%dT%H:%M:%SZ'))
        return start_date, end_date, dates
    else:
        return None, None, None

def add_to_pygeoapi_config(template_resource, template_params, pygeoapi_config):
    # This function adds postgis tables to the pygeoapi config file

    # Read the template file
    with open(template_resource, "r") as file:
        template = file.read()

    # Replace placeholders with real values
    for key, value in template_params.items():
        template = template.replace(key, value)
   
    # Append filled tempalte to the config file
    with open(pygeoapi_config, "a") as file:
        file.write(template)
        print(f"Table {template_params['<placeholder_table_name>']} added to the pygeoapi config file")

def download_page(data_url, page_no):
    data_url = data_url.replace('page=1', f'page={page_no}')
    gdf = gpd.read_file(data_url)
    return gdf

def get_occurrence_data(data_url, multiprocessing=True, pages="all"):
    # Loop over API pages and return the data as GeoDataFrame
    print("Retrieving occurrence data from the API...")
    if pages == 'all':
        last_page = get_last_page(data_url)
    else:
        last_page = int(pages)

    gdf = gpd.GeoDataFrame()
    if multiprocessing==True:
        with concurrent.futures.ProcessPoolExecutor() as executor:
            progress_bar = tqdm(total=last_page)
            futures = [executor.submit(download_page, data_url, page_no) for page_no in range(1, last_page + 1)]
            for future in concurrent.futures.as_completed(futures):
                gdf = pd.concat([gdf, future.result()], ignore_index=True)
                progress_bar.update(1)
            progress_bar.close()
    else:
        for page_no in range(1,last_page+1):
            next_gdf = tqdm(download_page(data_url, page_no))
            gdf = pd.concat([gdf, next_gdf])

    gdf = gdf[['unit.linkings.taxon.id','unit.linkings.taxon.scientificName','unit.linkings.taxon.vernacularName.en','unit.unitId','gathering.displayDateTime','geometry']]
    return gdf

def get_taxon_data(taxon_id_url, taxon_name_url, pages='all'):
    # Loop over API pages and return data as Dataframe
    print("Retrieving taxon data from the API...")
    if pages == 'all':
        last_page = get_last_page(taxon_id_url)
    else:
        last_page = int(pages)

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
        # Extract numeric part and convert to integer for each value in the list
        if type(row) is list:
            numeric_values = [int(value.split('.')[1]) for value in row]
            min_value = 'MVL.' + str(min(numeric_values))
        else:
            min_value = str(row)
        # Find the minimum value
        
        return min_value

    # Apply the function to each row and store the result in a new column
    id_df['mainTaxon'] = id_df['informalTaxonGroups'].apply(find_main_taxon)

    response = requests.get(taxon_name_url)
    json_data_results = response.json().get('results', [])
    name_df = pd.json_normalize(json_data_results)

    df = pd.merge(id_df, name_df, left_on='mainTaxon', right_on='id')
    return df

def main():
    # Get the data sets
    gdf = get_occurrence_data(occurrence_url, multiprocessing=True, pages=1)
    taxon_df = get_taxon_data(taxon_id_url, taxon_name_url, pages='all')

    # Merge taxonomy information to the occurrence data
    gdf['unit.linkings.taxon.id'] = gdf['unit.linkings.taxon.id'].str.extract('(MX\.\d+)')
    gdf = gdf.merge(taxon_df, left_on='unit.linkings.taxon.id', right_on='id_x', how='left')

    # Translate column names to comply with the darwin core standard
    gdf = column_names_as_dwc(gdf, lookup_table)

    # Connect to the PostGIS database
    print("Creating database connection...")
    engine = create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")

    # Create a new database if not exists
    #if not database_exists(engine.url):
    #    create_database(engine.url)

    # Add Postgis plugin if not added
    with engine.connect() as connection:
        connection.execute(text('CREATE EXTENSION IF NOT EXISTS postgis;'))
        connection.commit()

    # Iterate over unique values of the "class" attribute
    tot_rows = 0
    no_family_name = gdf[gdf['name'].isnull()]

    print("Looping over all species classes...")
    for group_name in gdf['name'].unique():
        # Filter the GeoDataFrame based on species group
        sub_gdf = gdf[gdf['name'] == group_name]

        # Get cleaned table name, bounding box (bbox) and number of rows (n_rows)
        table_name = clean_table_name(group_name)
        if table_name != 'nan':
            bbox = get_bbox(sub_gdf)
            min_date, max_date, dates = get_min_and_max_dates(sub_gdf)
            sub_gdf['datetimestamp'] = dates.astype(str)
            n_rows = len(sub_gdf)
            tot_rows += n_rows

            # Create parameters dictionary to fill the template for pygeoapi config file
            template_params = {
                "<placeholder_table_name>": table_name,
                "<placeholder_bbox>": str(bbox),
                "<placeholder_min_date>": min_date,
                "<placeholder_max_date>": max_date
            }
            # Create a postgis table 
            add_to_pygeoapi_config(template_resource, template_params, pygeoapi_config)

            # Add postgis table information to the pygeoapi config
            sub_gdf.to_postgis(table_name, engine, if_exists='replace', schema='public', index=False)

            print(f"In total {n_rows} rows of {table_name} inserted to the database and pygeoapi config file")


    print(f"In total {tot_rows} rows of data inserted successfully into the PostGIS database and pygeoapi config file.")
    print(f"Warning: in total {len(no_family_name)} species without scientific family name were discarded")
    print("API is ready to use.")

if __name__ == '__main__':
    main()