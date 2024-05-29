from sqlalchemy import inspect
from sqlalchemy import create_engine, text, MetaData
import geopandas as gpd
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
db_params = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': '5432',
    'pages': os.getenv('PAGES')
}

# Connect to the PostGIS database
engine = create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")
with engine.connect() as connection:
    connection.execute(text('CREATE EXTENSION IF NOT EXISTS postgis;'))
    connection.commit()

def drop_all_tables(engine=engine):
    """
    Drops all tables in the database except default PostGIS tables.

    Parameters:
    engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine for database connection
    """
    print("Clearing the database for new data...")
    
    postgis_default_tables = ['spatial_ref_sys',
                               'topology',
                               'layer',
                               'featnames',
                               'geocode_settings',
                               'geocode_settings_default',
                               'direction_lookup',
                               'secondary_unit_lookup', 
                               'state_lookup',
                               'street_type_lookup',
                               'place_lookup',
                               'county_lookup',
                               'countysub_lookup',
                               'zip_lookup_all',
                               'zip_lookup_base',
                               'zip_lookup',
                               'county',
                               'state',
                               'place',
                               'zip_state',
                               'zip_state_loc',
                               'cousub',
                               'edges',
                               'addrfeat',
                               'addr',
                               'zcta5',
                               'tabblock20',
                               'faces',
                               'loader_platform',
                               'loader_variables',
                               'loader_lookuptables',
                               'tract',
                               'tabblock',
                               'bg',
                               'pagc_gaz',
                               'pagc_lex',
                               'pagc_rules']

    # Find all table names
    metadata = MetaData()
    metadata.reflect(engine)
    all_tables =  metadata.tables.keys()

    # Loop over tables and try to drop them
    for table_name in all_tables:
        if table_name not in postgis_default_tables:
            metadata.tables[table_name].drop(engine)


def get_all_tables(engine):
    """
    Retrieves and prints all table names in the database.

    Parameters:
    engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine for database connection.
    """
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    # Print the list of tables
    print("Printing all tables in the database:")
    for table in tables:
        print(table)


def main():
    """
    Main function for managing the PostGIS database.
    Provides a user interface to list tables or drop all tables in the database.
    Not used normally
    """
    while True:
        # Prompt user for input
        choice = input("\nEnter the number of the function you want to use:\n"
                       "1. List all tables\n"
                       "2. Drop all tables\n"
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
        else:
            print("Invalid choice. Please enter a valid number and press Enter.")

    connection.close()
    engine.dispose()
    print("Done.")

if __name__ == "__main__":
    main()