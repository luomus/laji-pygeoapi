import geopandas as gpd
from sqlalchemy import create_engine, text
from sqlalchemy_utils import database_exists, create_database
import re

print("start")
# Path to the GeoJSON file
gpkd = r'scripts\HBF_86582_geo.gpkg'

# Database connection parameters
db_params = {
    'dbname': 'my_geospatial_db',
    'user': 'postgres',
    'password': 'admin123',
    'host': 'localhost',
    'port': '5433'
}

# Read file
gdf = gpd.read_file(gpkd, rows=10000)
crs = gdf.crs
gdf = gdf[['record_id', 'reported_name', 'common_name_finnish', 'scientific_name_interpreted', 'informal_groups_finnish', 'informal_groups_english', 'geometry']]

# Split 
gdf['class'] = gdf['informal_groups_english'].str.split(';', expand=True)[0]
print(gdf.head())

# Connect to the PostGIS database
engine = create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")
print("Connection created")

# Create a new database if not exist
if not database_exists(engine.url):
    create_database(engine.url)

# Add Postgis plugin if not added
with engine.connect() as connection:
    connection.execute(text('CREATE EXTENSION IF NOT EXISTS postgis;'))
    connection.commit()

# Function to clean the group value and return a table name
def clean_table_name(group_value):
    if group_value is None or group_value == '':
        return 'unclassified'
    cleaned_value = re.sub(r'[^\w\s]', '', group_value)  # Remove non-alphanumeric characters
    cleaned_value = cleaned_value.replace(' ', '_')  
    if len(cleaned_value) > 40:
        cleaned_value = cleaned_value[:40]
        print(f"Shorten name is {cleaned_value}")
    return f'{cleaned_value}'

# Iterate over unique values of the "informal_groups_english" attribute
for group_value in gdf['class'].unique():
    # Filter the GeoDataFrame for the current group
    group_gdf = gdf[gdf['class'] == group_value]
    n_rows = len(group_gdf)

    # Define the table name based on the group name
    table_name = clean_table_name(group_value)

    # Create a PostGIS table 
    group_gdf.to_postgis(table_name, engine, if_exists='replace', schema='public', index=False)

    print(f"In total {n_rows} rows of {table_name} inserted to the database")


print("All data inserted successfully into the PostGIS database.")