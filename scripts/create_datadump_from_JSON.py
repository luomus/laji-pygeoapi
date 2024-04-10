import geopandas as gpd
from sqlalchemy import create_engine, text
from sqlalchemy_utils import database_exists, create_database

# Path to the GeoJSON file
geojson_file = r'scripts/10000_species.geo.json'

# Database connection parameters
db_params = {
    'dbname': 'my_geospatial_db',
    'user': 'postgres',
    'password': 'admin123',
    'host': 'localhost',
    'port': '5432'
}

# Table name in the database
table_name = 'occurrence_data'

# Connect to the PostGIS database
engine = create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")

# Create a new database if not exist
if not database_exists(engine.url):
    create_database(engine.url)

with engine.connect() as connection:
    connection.execute(text('CREATE EXTENSION IF NOT EXISTS postgis;'))
    connection.commit()

# Read GeoJSON file into a GeoDataFrame and add id column
gdf = gpd.read_file(geojson_file)
gdf['id'] = range(1, len(gdf) + 1)
print(gdf.head())

# Insert data into the PostGIS database
gdf.to_postgis(table_name, engine, if_exists='replace', index=False)

print("Data inserted successfully into the PostGIS database.")
