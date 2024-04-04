import geopandas as gpd
from sqlalchemy import create_engine, text
from sqlalchemy_utils import database_exists, create_database

# Path to the GeoJSON file
geojson_file = r'C:/Users/alpoturu/pygeoapi_docker/test_db/10000_species.geo.json'

# Database connection parameters
db_params = {
    'dbname': 'my_geospatial_db',
    'user': 'postgres',
    'password': '',
    'host': 'localhost',
    'port': '5432'
}

# Table name in the database
table_name = 'occurrence_data'

# Read GeoJSON file into a GeoDataFrame
gdf = gpd.read_file(geojson_file)
print(gdf.head())

# Connect to the PostGIS database
engine = create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")

# Create a database if not exist
if not database_exists(engine.url):
    create_database(engine.url)

# Insert data into the PostGIS database
gdf.to_postgis(table_name, engine, if_exists='replace', index=False)

print("Data inserted successfully into the PostGIS database.")
