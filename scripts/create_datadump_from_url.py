import geopandas as gpd
from sqlalchemy import create_engine, text
from sqlalchemy_utils import database_exists, create_database
import re
import pandas as pd
from tqdm import tqdm
import requests

print("Start")
# Path to the GeoJSON file
data_url = r'https://laji.fi/api/warehouse/query/unit/list?administrativeStatusId=MX.finlex160_1997_appendix4_2021,MX.finlex160_1997_appendix4_specialInterest_2021,MX.finlex160_1997_appendix2a,MX.finlex160_1997_appendix2b,MX.finlex160_1997_appendix3a,MX.finlex160_1997_appendix3b,MX.finlex160_1997_appendix3c,MX.finlex160_1997_largeBirdsOfPrey,MX.habitatsDirectiveAnnexII,MX.habitatsDirectiveAnnexIV,MX.birdsDirectiveStatusAppendix1,MX.birdsDirectiveStatusMigratoryBirds&redListStatusId=MX.iucnCR,MX.iucnEN,MX.iucnVU,MX.iucnNT&countryId=ML.206&time=1990-01-01/&aggregateBy=gathering.conversions.wgs84Grid05.lat,gathering.conversions.wgs84Grid1.lon&onlyCount=false&individualCountMin=0&coordinateAccuracyMax=1000&page=1&pageSize=10000&taxonAdminFiltersOperator=OR&collectionAndRecordQuality=PROFESSIONAL:EXPERT_VERIFIED,COMMUNITY_VERIFIED,NEUTRAL,UNCERTAIN;HOBBYIST:EXPERT_VERIFIED,COMMUNITY_VERIFIED,NEUTRAL;AMATEUR:EXPERT_VERIFIED,COMMUNITY_VERIFIED;&geoJSON=true&featureType=ORIGINAL_FEATURE'
#data_url = r'test_data\10000_virva_data.json'
taxon_file = r'scripts\taxon-export.tsv'
template_resource = r'scripts\template_resource.txt'
pygeoapi_config = r'pygeoapi-config.yml'

# Database connection parameters
db_params = {
    'dbname': 'my_geospatial_db',
    'user': 'postgres',
    'password': 'admin123',
    'host': 'localhost',
    'port': '5433'
}

def get_last_page(data_url):
    # Get the last page number
    try:
        response = requests.get(data_url)
        api_response = response.json()
        return(api_response.get("lastPage"))
    except Exception as e:
        print("An error occurred:", e)

def clean_table_name(group_name):
    # Function to clean and return a table name 
    
    # If name not exists
    if group_name is None or group_name =='nan' or group_name == '':
        return 'unclassified'
    
    # Remove non-alphanumeric characters and white spaces
    cleaned_name = re.sub(r'[^\w\s]', '', str(group_name)).replace(' ', '_')   

    # Shorten the name
    if len(cleaned_name) > 40:
        cleaned_name = cleaned_name[:40]

    return f'{cleaned_name}'

def get_bbox(sub_gdf):
    minx, miny, maxx, maxy = sub_gdf.geometry.total_bounds
    return [minx, miny, maxx, maxy]

def add_to_pygeoapi_config(template_resource, template_params, pygeoapi_config):
    # This function adds data sets to the pygeoapi config file

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

# Loop over API pages and read the data 
print("Retrieving data from the API...")
# last_page = get_last_page(data_url)
last_page = 3
gdf = gpd.GeoDataFrame()
for page_no in tqdm(range(1,last_page+1)):
    next_page = data_url.replace('page=1', f'page={page_no}')
    next_gdf = gpd.read_file(next_page, engine="pyogrio")
    gdf = pd.concat([gdf, next_gdf])

# Merge taxonomy information to the geodataframe
taxonomy_df = pd.read_table(taxon_file, sep='\t')
gdf = gdf.merge(taxonomy_df, left_on='unit.linkings.taxon.scientificName', right_on='Scientific name', how='left')

# Connect to the PostGIS database
print("Creating database connection...")
engine = create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")

# Create a new database if not exists
if not database_exists(engine.url):
    create_database(engine.url)

# Add Postgis plugin if not added
with engine.connect() as connection:
    connection.execute(text('CREATE EXTENSION IF NOT EXISTS postgis;'))
    connection.commit()

# Iterate over unique values of the "class" attribute
tot_rows = 0
no_family_name = gdf[gdf['Class, Scientific name'].isnull()]

print("Looping over all species classes...")
for group_name in gdf['Class, Scientific name'].unique():
    
    # Filter the GeoDataFrame based on species
    sub_gdf = gdf[gdf['Class, Scientific name'] == group_name]
    
    # Get cleaned table name, bounding box (bbox) and number of rows (n_rows)
    table_name = clean_table_name(group_name)
    bbox = get_bbox(sub_gdf)
    n_rows = len(sub_gdf)
    tot_rows += n_rows

    template_params = {
        "<placeholder_table_name>": table_name,
        "<placeholder_bbox>": str(bbox)
    }

    # Create a postgis table and add to the pygeoapi config. file
    #add_to_pygeoapi_config(template_resource, template_params, pygeoapi_config)
    #sub_gdf.to_postgis(table_name, engine, if_exists='replace', schema='public', index=False)

    print(f"In total {n_rows} rows of {table_name} inserted to the database and pygeoapi config file")


print(f"In total {tot_rows} rows of data inserted successfully into the PostGIS database and pygeoapi config file.")
print(f"In total {len(no_family_name)} species without scientific family name were discarded")
print("API is ready to use.")