from sqlalchemy import inspect
from sqlalchemy import create_engine, text, MetaData
import geopandas as gpd
import pandas as pd
import os
from dotenv import load_dotenv
from process_data import get_min_max_dates, clean_table_name

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

def drop_all_tables():
    """
    Drops all tables in the database except default PostGIS tables.
    """
    print("Initializing the database...")
    
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

def get_all_tables():
    """
    Retrieves and prints all table names in the database.
    """
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    # Print the list of tables
    print("Printing all tables in the database:")
    for table in tables:
        print(table)

def get_table_bbox(table_name):
    """
    Retrieve the bounding box (bbox) of all features in a PostGIS table.

    Parameters:
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

def get_table_dates(table_name):
    """
    Retrieve the earliest and latest event dates from a specified table.

    Parameters:
    table_name (str): The name of the table to query.

    Returns:
    tuple: A tuple containing the minimum and maximum dates.
    """
    sql = f'SELECT "Keruu_aloitus_pvm", "Keruu_lopetus_pvm", "geometry" FROM "{table_name}"'
    gdf = gpd.read_postgis(sql, engine, geom_col='geometry')
    min_date, max_date = get_min_max_dates(gdf)
    return min_date, max_date

def get_amount_of_occurrences(table_name):
    """
    Retrieve the number of occurrences in a given table

    Parameters:
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

def to_db(gdf, pending_occurrences, table_names, failed_features_count, occurrences_without_group_count, last_iteration=False):
    """
    Process and insert geospatial data into a PostGIS database.

    Parameters:
    gdf (GeoDataFrame): The main GeoDataFrame containing occurrences.
    pending_occurrences (DataFrame): A DataFrame to store pending occurrences.
    table_names (list): A list to store table names that have been processed.
    failed_features_count (int): A counter for failed occurrence inserts.
    occurrences_without_group_count (int): A counter for occurrences without a group
    last_iteration (bool): Flag indicating whether this is the last iteration.
    """
    
    # Process each unique group
    occurrences_without_group_count += gdf['Elioryhma'].isnull().sum()
    gdf = gdf.dropna(subset=['Elioryhma'])

    # If it is the last round, add all pending occurrences to the gdf
    if last_iteration:
        gdf = pd.concat([gdf, pending_occurrences], axis=0)

    unique_groups = gdf['Elioryhma'].unique()
    for group_name in unique_groups:
        table_name = clean_table_name(group_name)
        if table_name:
            # Filter the sub DataFrame
            sub_gdf = gdf[gdf['Elioryhma'] == group_name]

            # If only a couple of occurrences in a group, skip them and insert later to the database to save time
            if not last_iteration and len(sub_gdf) < 100:
                pending_occurrences = pd.concat([pending_occurrences, sub_gdf], axis=0)
                continue

            sub_gdf = sub_gdf.assign(Paikallinen_tunniste=sub_gdf.index)
            try:
                with engine.connect() as conn:
                    sub_gdf.to_postgis(table_name, engine, if_exists='append', schema='public', index=False)
                if table_name not in table_names:
                    table_names.append(table_name)
            except Exception as e:
                print(f"Error occurred: {e}")
                failed_features_count += len(sub_gdf)

            del sub_gdf

    return pending_occurrences, table_names, failed_features_count, occurrences_without_group_count

def update_indexes(table_name):
    """
    Updates spatial and normal indexes for the given table

    Parameters:
    table_name (str): A PostGIS table name to where indexes will be updated
    """

    with engine.connect() as connection:
        reindex_sql = text(f'REINDEX TABLE "{table_name}";')
        connection.execute(reindex_sql)

        spatial_reindex_sql = text(f'CREATE INDEX "{table_name}_geom_x" ON "{table_name}" USING GIST (geometry);')
        connection.execute(spatial_reindex_sql)

def validate_geometries_postgis(table_name):
    """
    Finds and fix invalid geometries 

    Parameters:
    table_name (str): Table to be checked
    """

    with engine.connect() as connection:
        count_fixed_sql = text(f'UPDATE "{table_name}" SET geometry = ST_MakeValid(geometry) WHERE NOT ST_IsValid(geometry);')
        result = connection.execute(count_fixed_sql)
        edited_features_count = result.rowcount
    return edited_features_count


engine = connect_to_db()