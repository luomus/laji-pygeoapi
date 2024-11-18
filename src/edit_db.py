from sqlalchemy import inspect
from sqlalchemy import create_engine, text, MetaData
import pandas as pd
import os
from dotenv import load_dotenv
import concurrent.futures
import multiprocessing
import time
from sqlalchemy.dialects.postgresql import base
from geoalchemy2.types import Geometry

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

    # Register the Geometry type with SQLAlchemy
    base.ischema_names['geometry'] = Geometry

    return engine

def drop_all_tables():
    """
    Drops all tables in the database except default PostGIS tables.
    """
    print("Emptying the database...")
    
    # Find all table names
    metadata = MetaData()
    metadata.reflect(engine)
    all_tables =  metadata.tables.keys()

    # Loop over tables and try to drop them
    for table_name in all_tables:
        if table_name not in postgis_default_tables:
            metadata.tables[table_name].drop(engine)

def drop_table(table_names):
    """
    Drops one table in the database.

    Parameters:
    table_names (list): The table names
    """
    
    # Find all table names
    metadata = MetaData()
    metadata.reflect(engine)

    for i in table_names:
        try: 
            metadata.tables[i].drop(engine)
        except:
            pass # If data update didn't succeed last time, it's possible the table doesnt't exist and can't be dropped

def get_all_tables():
    """
    Retrieves all table names (except default tables) from the database. Returns them as a list.
    """
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    tables_without_defaults = [i for i in tables if i not in postgis_default_tables]

    return tables_without_defaults

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
    
def get_quality_frequency(table_name):
    """
    Retrieve frequencies of the column Aineiston_laatu. Results can be stored to metadata later.

    Parameters:
    table_name (str): The name of the table to query.

    """
    sql = text(f'''
    SELECT
        "Aineiston_laatu",
        ROUND((COUNT(*)::decimal / SUM(COUNT(*)) OVER ()) * 100, 2) AS percentage
    FROM
        "{table_name}"
    GROUP BY
        "Aineiston_laatu";
    ''')

    quality_dict = {}
    
    with engine.connect() as connection:
        result = connection.execute(sql)

    for row in result:
        quality_value, quality_percentage = row
        quality_dict[quality_value] = float(quality_percentage)

    return quality_dict

def get_table_dates(table_name):
    """
    Retrieve the earliest and latest dates from a specified table.

    Parameters:
    table_name (str): The name of the table to query.

    Returns:
    tuple: A tuple containing the minimum and maximum dates in RFC3339 format.
    """
    sql = text(f'''
    SELECT 
        TO_CHAR(MIN("Keruu_aloitus_pvm"), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as min_date,
        TO_CHAR(MAX("Keruu_lopetus_pvm"), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as max_date
    FROM "{table_name}"
    WHERE "Keruu_aloitus_pvm" IS NOT NULL OR "Keruu_lopetus_pvm" IS NOT NULL;
    ''')

    with engine.connect() as connection:
        result = connection.execute(sql).fetchone()

    # Result will contain the minimum and maximum dates
    min_date, max_date = result
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

def get_amount_of_all_occurrences():
    """The function to return the number of all occurrences from the database."""  
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    number_of_all_occurrences = 0
    for table in tables:
        if table not in postgis_default_tables:
            # Specify the column that contains the occurrences. Replace 'occurrences_column' with the actual column name.
            sql = f'SELECT COUNT(*) as total_occurrences FROM "{table}"'

            # Read the result into a DataFrame
            result_df = pd.read_sql_query(sql, engine)

            # Get the total occurrences from the DataFrame
            number_of_all_occurrences += result_df['total_occurrences'].iloc[0]

    return number_of_all_occurrences

def to_db(gdf, table_names):
    """
    Process and insert geospatial data into a PostGIS database.

    Parameters:
    gdf (GeoDataFrame): The main GeoDataFrame containing occurrences.
    table_names (list): DB table names

    Returns:
    failed_features_count  (int): An updated counter for failed occurrence inserts.
    """

    # Separate by geometry type
    geom_types = {
        table_names[0]: gdf[gdf.geometry.geom_type.isin(['Point','MultiPoint'])],
        table_names[1]: gdf[gdf.geometry.geom_type.isin(['LineString', 'MultiLineString'])],
        table_names[2]: gdf[gdf.geometry.geom_type.isin(['Polygon', 'MultiPolygon'])]
    }

    failed_features_count = 0

    # Loop over geometry types and corresponding geodataframes
    for table_name, geom_gdf in geom_types.items():
        try:
            with engine.connect() as conn:
                geom_gdf.to_postgis(table_name, conn, if_exists='append', schema='public', index=False)
        except Exception as e:
            print(f"Error occurred: {e}")
            failed_features_count = len(geom_gdf)

    return failed_features_count

def update_single_table_indexes(table_name):
    with engine.connect() as connection:
        print(f"Updating indexes for table: {table_name}")

        # Combine all index creation queries into a single execution
        index_creation_sql = text(f'''
            CREATE INDEX IF NOT EXISTS "idx_{table_name}_Kunta" ON "{table_name}" ("Kunta");
            CREATE INDEX IF NOT EXISTS "idx_{table_name}_geom" ON "{table_name}" USING GIST (geometry);
        ''')

        # Execute the combined index creation block
        connection.execute(index_creation_sql)
        connection.commit()

def update_indexes(table_names, use_multiprocessing=True):
    """
    Updates spatial and normal indexes for the given table

    Parameters:
    table_names (list): A PostGIS table names to where indexes will be updated
    """
    if table_names:
       
        if use_multiprocessing:
            with concurrent.futures.ProcessPoolExecutor() as executor:
                executor.map(update_single_table_indexes, table_names)
        else:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.map(update_single_table_indexes, table_names)
    else:
        print("No table names given, can't update table indexes")

def validate_geometries_postgis(table_name):
    """
    Finds and fix invalid geometries 

    Parameters:
    table_name (str): Table to be checked

    Returns:
    edited_features_count (int): the number of features that hav been fixed
    """
    if 'point' not in table_name:
        with engine.connect() as connection:
            count_fixed_sql = text(f'UPDATE "{table_name}" SET geometry = ST_MakeValid(geometry) WHERE NOT ST_IsValid(geometry);')
            try:
                result = connection.execute(count_fixed_sql)
                edited_features_count = result.rowcount
                connection.commit()
            except Exception as e:
                print(e)
                edited_features_count = 0
        return edited_features_count
    else:
        return 0

def remove_duplicates(table_names):
    """
    Remove duplicate rows from the specified table based on the 'Havainnon_tunniste' attribute.
    This is needed if data is updated and some pages have had occurrences that are already in the database.

    Parameters:
    table_names (list): The names of the tables to be checked for duplicates.

    Returns:
    int: The number of duplicate rows removed.
    """
    if table_names:
        removed_occurrences = 0
        for table_name in table_names:
            number_before_deletion = get_amount_of_occurrences(table_name)

            with engine.connect() as connection:
                # SQL query to remove duplicates
                remove_duplicates_sql = text(f'''
                    DELETE FROM "{table_name}" t
                    USING (
                        SELECT "Havainnon_tunniste", MIN(ctid) AS keep_ctid
                        FROM "{table_name}"
                        GROUP BY "Havainnon_tunniste"
                    ) cte
                    WHERE t.ctid != cte.keep_ctid AND t."Havainnon_tunniste" = cte."Havainnon_tunniste";
                ''')

                # Execute the SQL to remove duplicates
                connection.execute(remove_duplicates_sql)
                connection.commit()

            number_after_deletion = get_amount_of_occurrences(table_name)
            removed_occurrences += number_before_deletion - number_after_deletion

        return removed_occurrences
    else: 
        return 0


engine = connect_to_db()