import geopandas as gpd
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy_utils import database_exists, create_database
import requests, pyogrio, psycopg2, geoalchemy2, os, concurrent.futures
import edit_config, edit_db, process_data


pd.options.mode.copy_on_write = True
gpd.options.io_engine = "pyogrio" # Faster way to read data

occurrence_url = r'https://laji.fi/api/warehouse/query/unit/list?administrativeStatusId=MX.finlex160_1997_appendix4_2021,MX.finlex160_1997_appendix4_specialInterest_2021,MX.finlex160_1997_appendix2a,MX.finlex160_1997_appendix2b,MX.finlex160_1997_appendix3a,MX.finlex160_1997_appendix3b,MX.finlex160_1997_appendix3c,MX.finlex160_1997_largeBirdsOfPrey,MX.habitatsDirectiveAnnexII,MX.habitatsDirectiveAnnexIV,MX.birdsDirectiveStatusAppendix1,MX.birdsDirectiveStatusMigratoryBirds&redListStatusId=MX.iucnCR,MX.iucnEN,MX.iucnVU,MX.iucnNT&countryId=ML.206&time=1990-01-01/&aggregateBy=gathering.conversions.wgs84Grid05.lat,gathering.conversions.wgs84Grid1.lon&onlyCount=false&individualCountMin=0&coordinateAccuracyMax=1000&page=1&pageSize=10000&taxonAdminFiltersOperator=OR&collectionAndRecordQuality=PROFESSIONAL:EXPERT_VERIFIED,COMMUNITY_VERIFIED,NEUTRAL,UNCERTAIN;HOBBYIST:EXPERT_VERIFIED,COMMUNITY_VERIFIED,NEUTRAL;AMATEUR:EXPERT_VERIFIED,COMMUNITY_VERIFIED;&geoJSON=true&featureType=ORIGINAL_FEATURE'
#occurrence_file = r'test_data/10000_virva_data.json'
taxon_id_url= r'https://laji.fi/api/taxa/MX.37600/species?onlyFinnish=true&selectedFields=id,vernacularName,scientificName,informalTaxonGroups&lang=multi&page=1&pageSize=1000&sortOrder=taxonomic'
#taxon_id_file = r'test_data/taxon-export.tsv'
taxon_name_url = r'https://laji.fi/api/informal-taxon-groups?pageSize=1000'
#taxon_name_file = r'test_data/informal-taxon-groups.json'
template_resource = r'template_resource.txt'
pygeoapi_config = r'pygeoapi-config.yml'
lookup_table = r'lookup_table_columns.csv'

# Load environment variables from .env file
load_dotenv()
db_params = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': 'postgres',
    'port': '5432'
}

def get_last_page(data_url):
    # Get the last page number from API
    try:
        response = requests.get(data_url)
        api_response = response.json()
        return(api_response.get("lastPage"))
    except Exception as e:
        print("An error occurred:", e)

def download_page(data_url, page_no):
    data_url = data_url.replace('page=1', f'page={page_no}')
    gdf = gpd.read_file(data_url)
    return gdf

def get_occurrence_data(data_url, multiprocessing=True, pages="all"):
    # Loop over API pages and return the data as GeoDataFrame
    
    if pages == 'all':
        last_page = get_last_page(data_url)
    else:
        last_page = int(pages)
    
    print(f"Retrieving {last_page} pages of occurrence data from the API...")

    gdf = gpd.GeoDataFrame()
    if multiprocessing==True:
        with concurrent.futures.ProcessPoolExecutor() as executor:
            futures = [executor.submit(download_page, data_url, page_no) for page_no in range(1, last_page + 1)]
            progress_bar = tqdm(total=last_page)
            for future in tqdm(concurrent.futures.as_completed(futures)):
                progress_bar.update(1)
                gdf = pd.concat([gdf, future.result()], ignore_index=True)
            progress_bar.close()
    else:
        for page_no in tqdm(range(1,last_page+1)):
            next_gdf = download_page(data_url, page_no)
            gdf = pd.concat([gdf, next_gdf])

    gdf = gdf[['unit.linkings.taxon.id','unit.linkings.taxon.scientificName','unit.linkings.taxon.vernacularName.en','unit.unitId','gathering.displayDateTime','geometry']]
    return gdf

def get_taxon_data(taxon_id_url, taxon_name_url, pages='all'):
    # Loop over API pages and return data as Dataframe
    
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
    print("Start")

    # Get the data sets
    gdf = get_occurrence_data(occurrence_url, multiprocessing=True, pages=1) # or '10000_virva_data.json' if local test data only
    taxon_df = get_taxon_data(taxon_id_url, taxon_name_url, pages='all') # or taxon_df = pd.read_csv('tmp.csv') if local test data only

    # Merge taxonomy information to the occurrence data
    print("Joining data sets together...")
    gdf['unit.linkings.taxon.id'] = gdf['unit.linkings.taxon.id'].str.extract('(MX\.\d+)')
    gdf = gdf.merge(taxon_df, left_on='unit.linkings.taxon.id', right_on='id_x', how='left')

    # Translate column names to comply with the darwin core standard
    gdf = process_data.column_names_to_dwc(gdf, lookup_table)

    # Connect to the PostGIS database
    #print("Creating database connection...")
    #engine = create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")
    #with engine.connect() as connection:
    #    connection.execute(text('CREATE EXTENSION IF NOT EXISTS postgis;'))
    #    connection.commit()

    # Iterate over unique values of the "class" attribute
    tot_rows = 0
    no_family_name = gdf[gdf['name'].isnull()]

    # Clear config file and database to make space for new data sets
    #edit_config.clear_collections_from_config(pygeoapi_config)
    #edit_db.drop_all_tables(engine)

    print("Looping over all species classes...")
    for group_name in gdf['name'].unique():
        # Filter the GeoDataFrame based on species group
        sub_gdf = gdf[gdf['name'] == group_name]

        # Get cleaned table name, bounding box (bbox) and number of rows (n_rows)
        table_name = process_data.clean_table_name(group_name)
        if table_name != 'nan':
            bbox = process_data.get_bbox(sub_gdf)
            min_date, max_date, dates = process_data.get_min_and_max_dates(sub_gdf)
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
            # Add database information into the config file
            #edit_config.add_to_pygeoapi_config(template_resource, template_params, pygeoapi_config)

            # Add data to the database
            #sub_gdf.to_postgis(table_name, engine, if_exists='replace', schema='public', index=False)

            print(f"In total {n_rows} rows of {table_name} inserted to the database and pygeoapi config file")


    print(f"\nIn total {tot_rows} rows of data inserted successfully into the PostGIS database and pygeoapi config file.")
    print(f"Warning: in total {len(no_family_name)} species without scientific family name were discarded")
    print("API is ready to use.")

if __name__ == '__main__':
    main()