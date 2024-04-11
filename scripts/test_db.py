from sqlalchemy import inspect
from sqlalchemy import create_engine, text, MetaData
import geopandas as gpd
import pandas as pd

# Database connection parameters
db_params = {
    'dbname': 'my_geospatial_db',
    'user': 'postgres',
    'password': 'admin123',
    'host': 'localhost',
    'port': '5433'
}

# Connect to the PostGIS database
engine = create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")



with engine.connect() as connection:
    connection.execute(text('CREATE EXTENSION IF NOT EXISTS postgis;'))
    connection.commit()

def drop_all_tables(engine):
    # Find all table names
    metadata = MetaData()
    metadata.reflect(engine)
    all_tables =  metadata.tables.keys()

    # Loop over tables and try to drop them.
    for table_name in all_tables:
        try:
            metadata.tables[table_name].drop(engine)
            print(f"table {table_name} dropped")
        except:
            print(f"Can't drop table {table_name}")

def get_all_tables(engine):
    # Get a list of tables in the database
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    # Print the list of tables
    print("Tables in the database:")
    for table in tables:
        print(table)

get_all_tables(engine)
#drop_all_tables(engine)



connection.close()
engine.dispose()
print("Done.")