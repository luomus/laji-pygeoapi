from sqlalchemy import inspect
from sqlalchemy import create_engine, text, MetaData
import geopandas as gpd
import pandas as pd
import os
from dotenv import load_dotenv

# Database connection parameters from the secret .env file
load_dotenv()
db_params = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER') ,
    'password': os.getenv('POSTGRES_PASSWORD') ,
    'host': 'localhost',
    'port': '5433'
}

pygeoapi_config = r'pygeoapi-config.yml'

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

def clear_collections_from_config(pygeoapi_config):
    # Read the contents of the file
    with open(pygeoapi_config, 'r') as file:
        lines = file.readlines()

    # Find the index of the line containing the keyword "resources"
    keyword_index = None
    for i, line in enumerate(lines):
        if "resources" in line:
            keyword_index = i
            break

    # If the keyword is found, keep only the lines before it
    if keyword_index is not None:
        lines = lines[:keyword_index+1]

    # Write the modified contents back to the file
    with open(pygeoapi_config, 'w') as file:
        file.writelines(lines)
        print("All collections removed from pygeoapi config file")




def main():
    while True:
        # Prompt user for input
        choice = input("Enter the number of the function you want to use:\n"
                       "1. List all tables\n"
                       "2. Drop all tables\n"
                       "3. Clear tables from pygeoapi config file\n"
                       "Enter 'q' to quit\n")

        # Check if user wants to quit
        if choice.lower() == 'q':
            print("Exiting...")
            break

        # Call the corresponding function based on user's choice
        if choice == '1':
            get_all_tables(engine)
        elif choice == '2':
            drop_all_tables(engine)
        elif choice == '3':
            clear_collections_from_config(pygeoapi_config)
        else:
            print("Invalid choice. Please enter a valid number and press Enter.")

    connection.close()
    engine.dispose()
    print("Done.")

if __name__ == "__main__":
    main()


