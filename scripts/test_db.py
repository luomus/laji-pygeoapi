from sqlalchemy import inspect
from sqlalchemy import create_engine, text
import geopandas as gpd
import pandas as pd

# Database connection parameters
db_params = {
    'dbname': 'my_geospatial_db',
    'user': 'postgres',
    'password': 'admin123',
    'host': 'localhost',
    'port': '5432'
}

# Table name in the database
table_name = 'postgis_dump'

# Connect to the PostGIS database
engine = create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")



with engine.connect() as connection:
    connection.execute(text('CREATE EXTENSION IF NOT EXISTS postgis;'))
    connection.commit()

# Get a list of tables in the database
inspector = inspect(engine)
tables = inspector.get_table_names()

# Print the list of tables
print("Tables in the database:")
for table in tables:
    print(table)

# Query the table and load it into a GeoDataFrame
sql = f"SELECT * FROM {table_name}"
gdf = gpd.read_postgis(sql, engine, geom_col='wkb_geometry')
print(gdf.head())
print(gdf.crs)

# Explicitly close the connection
connection.close()

# Dispose of the engine
engine.dispose()

print("Done.")
