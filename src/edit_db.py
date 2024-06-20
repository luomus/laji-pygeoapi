from sqlalchemy import inspect
from sqlalchemy import create_engine, text, MetaData
import geopandas as gpd
import pandas as pd
import os
from dotenv import load_dotenv
from process_data import get_min_max_dates


def connect_to_db():
    """
    Creates connection to the PostGIS database using credentials store in .env file or parameters/secrets in openshift

    Returns:
    engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine for database connection
    """
    # Load environment variables from .env file
    load_dotenv()
    db_params = {
        'dbname': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': os.getenv('POSTGRES_HOST'),
        'port': '5432'
    }

    # Connect to the PostGIS database
    print("Creating database connection...")
    engine = create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")
    with engine.connect() as connection:
        connection.execute(text('CREATE EXTENSION IF NOT EXISTS postgis;'))
        connection.commit()

    return engine

def drop_all_tables(engine):
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

def get_table_bbox(engine, table_name):
    """
    Retrieve the bounding box (bbox) of all features in a PostGIS table.

    Parameters:
    engine (sqlalchemy.engine.Engine): SQLAlchemy engine connected to the PostGIS database.
    table_name (str): Name of the table.

    Returns:
    tuple: Bounding box coordinates in the form [min_x, min_y, max_x, max_y].
    """
    sql = text(f'SELECT ST_Extent("geometry") FROM "{table_name}"')
    with engine.connect() as connection:
        result = connection.execute(sql)
        extent = result.scalar()

    if extent:
        extent = extent.replace('BOX(', '').replace(')', '')
        min_x, min_y, max_x, max_y = map(float, extent.split(',')[0].split(' ') + extent.split(',')[1].split(' '))
        return [min_x, min_y, max_x, max_y]
    else:
        return None

def get_table_dates(engine, table_name):
    """
    Retrieve the earliest and latest event dates from a specified table.

    Parameters:
    engine (sqlalchemy.engine.Engine): The SQLAlchemy engine connected to the database.
    table_name (str): The name of the table to query.

    Returns:
    tuple: A tuple containing the minimum and maximum dates.
    """
    sql = f'SELECT "Keruu_aloitus_pvm", "Keruu_lopetus_pvm", "geometry" FROM "{table_name}"'
    gdf = gpd.read_postgis(sql, engine, geom_col='geometry')
    min_date, max_date = get_min_max_dates(gdf)
    return min_date, max_date


def get_amount_of_occurrences(engine, table_name):
    """
    Retrieve the number of occurrences in a given table

    Parameters:
    engine (sqlalchemy.engine.Engine): The SQLAlchemy engine connected to the database.
    table_name (str): The name of the table to query.

    Returns:
    int: number of occurrences.
    """
    # Specify the column that contains the occurrences. Replace 'occurrences_column' with the actual column name.
    sql = f'SELECT COUNT(*) as total_occurrences FROM "{table_name}"'
    
    # Read the result into a DataFrame
    result_df = pd.read_sql_query(sql, engine)
    
    # Get the total occurrences from the DataFrame
    total_occurrences = result_df['total_occurrences'].iloc[0]

    return total_occurrences