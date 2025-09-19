from sqlalchemy import inspect, create_engine, text, MetaData
import pandas as pd
import os
import logging
from dotenv import load_dotenv
import concurrent.futures
from sqlalchemy.dialects.postgresql import base
from geoalchemy2.types import Geometry
from datetime import date

logger = logging.getLogger(__name__)

postgis_default_tables = [
    'spatial_ref_sys', 'topology', 'layer', 'featnames', 'geocode_settings',
    'geocode_settings_default', 'direction_lookup', 'secondary_unit_lookup',
    'state_lookup', 'street_type_lookup', 'place_lookup', 'county_lookup',
    'countysub_lookup', 'zip_lookup_all', 'zip_lookup_base', 'zip_lookup',
    'county', 'state', 'place', 'zip_state', 'zip_state_loc', 'cousub',
    'edges', 'addrfeat', 'addr', 'zcta5', 'tabblock20', 'faces',
    'loader_platform', 'loader_variables', 'loader_lookuptables', 'tract',
    'tabblock', 'bg', 'pagc_gaz', 'pagc_lex', 'pagc_rules', 'last_update'
]

_engine = None

def get_engine():
    global _engine
    if _engine is None:
        _engine = connect_to_db()
    return _engine

def get_and_update_last_update():
    """
    Retrieves the last update timestamp from the database and updates it.
    If the table does not exist, it will be created with the current date.

    Returns:
    str: The last update timestamp.
    """
    today_date = str(date.today())

    with get_engine().connect() as connection:
        # Ensure the table exists
        connection.execute(text("CREATE TABLE IF NOT EXISTS last_update (last_update DATE)"))


        # Fetch the current last update date
        result = connection.execute(text("SELECT last_update FROM last_update"))
        last_update = result.scalar()

        if last_update:
            # Update the date if it exists
            connection.execute(
                text("UPDATE last_update SET last_update = :today_date"),
                {"today_date": today_date}
            )
        else:
            # Insert the date if the table is empty
            connection.execute(
                text("INSERT INTO last_update (last_update) VALUES (:today_date)"),
                {"today_date": today_date}
            )

        connection.commit()

    return last_update

def connect_to_db():
    """
    Creates connection to the PostGIS database using credentials stored in .env file or parameters/secrets in openshift.

    Returns:
    engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine for database connection
    """
    # Remove this line, since env vars are set in test code
    load_dotenv()
    db_params = {
        'dbname': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': os.getenv('POSTGRES_HOST'),
        'port': os.getenv('POSTGRES_PORT', '5432')  # Default PostgreSQL port
    }

    # Connect to the PostGIS database
    logger.info("Creating database connection...")
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
    logger.info("Dropping all the tables from the database...")
    
    tables = get_all_tables()
    if not tables:
        return
    with get_engine().connect() as connection:
        for table_name in tables:
            try:
                connection.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
            except Exception as e:
                logger.warning(f"Failed to drop table {table_name}: {e}")
        connection.commit()

def drop_table(table_names):
    """
    Drops specified tables in the database.

    Parameters:
    table_names (list): The table names
    """
    if not table_names:
        return

    with get_engine().connect() as connection:
        for table_name in table_names:
            try:
                connection.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
            except Exception as e:
                logger.warning(f"Failed to drop table {table_name}: {e}")
        connection.commit()

def get_all_tables():
    """
    Retrieves all table names (except default tables) from the database. Returns them as a list.
    """
    inspector = inspect(get_engine())
    tables = inspector.get_table_names()
    return [table for table in tables if table not in postgis_default_tables]

def get_table_bbox(table_name):
    """
    Retrieve the bounding box (bbox) of all features in a PostGIS table.

    Parameters:
    table_name (str): Name of the table.

    Returns:
    tuple: Bounding box coordinates in the form [min_x, min_y, max_x, max_y].
    """
    sql = text(f'SELECT ST_Extent("geometry") FROM "{table_name}"')
    with get_engine().connect() as connection:
        extent = connection.execute(sql).scalar()

    if extent:
        extent = extent.replace('BOX(', '').replace(')', '')
        min_x, min_y, max_x, max_y = map(float, extent.split(',')[0].split(' ') + extent.split(',')[1].split(' '))
        return [min_x, min_y, max_x, max_y]
    return None
    
def get_quality_frequency(table_name):
    """
    Retrieve frequencies of the column Aineiston_laatu. Results can be stored to metadata later.

    Parameters:
    table_name (str): The name of the table to query.

    Returns:
    dict: A dictionary with quality values and their percentages.
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
    with get_engine().connect() as connection:
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

    with get_engine().connect() as connection:
        result = connection.execute(sql).fetchone()

    # Result will contain the minimum and maximum dates
    min_date, max_date = result if result else (None, None)
    return min_date, max_date

def check_table_exists(table_name):
    """
    Check if a table exists in the database.

    Parameters:
    table_name (str): The name of the table to check.

    Returns:
    bool: True if the table exists, False otherwise.
    """
    sql = text("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = :tname
        )
    """)
    with get_engine().connect() as connection:
        exists = connection.execute(sql, {"tname": table_name}).scalar()
    return bool(exists)

def get_amount_of_occurrences(table_name):
    """
    Retrieve the number of occurrences in a given table.

    Parameters:
    table_name (str): The name of the table to query.

    Returns:
    int: Number of occurrences.
    """
    sql = f'SELECT COUNT(*) as total_occurrences FROM "{table_name}"'
    
    # Read the result into a DataFrame
    result_df = pd.read_sql_query(sql, get_engine())
    return result_df['total_occurrences'].iloc[0]

def get_amount_of_all_occurrences():
    """
    Retrieve the number of all occurrences from the database.

    Returns:
    int: Total number of occurrences.
    """
    tables = get_all_tables()
    total_occurrences = sum(get_amount_of_occurrences(table) for table in tables)
    return total_occurrences

def to_db(gdf, table_names):
    """
    Process and insert geospatial data into a PostGIS database.

    Parameters:
    gdf (GeoDataFrame): The main GeoDataFrame containing occurrences.
    table_names (list): DB table names

    Returns:
    int: An updated counter for failed occurrence inserts.
    """
   
    # Set index
    if 'Paikallinen_tunniste' in gdf.columns:
        gdf = gdf.set_index('Paikallinen_tunniste', drop=True)

    # Separate by geometry type
    geom_types = {
        table_names[0]: gdf[gdf.geometry.geom_type.isin(['Point','MultiPoint'])],
        table_names[1]: gdf[gdf.geometry.geom_type.isin(['LineString', 'MultiLineString'])],
        table_names[2]: gdf[gdf.geometry.geom_type.isin(['Polygon', 'MultiPolygon'])]
    }

    failed_features_count = 0

    # Loop over geometry types and corresponding geodataframes
    with get_engine().connect() as conn:
        for table_name, geom_gdf in geom_types.items():
            try:
                geom_gdf.to_postgis(table_name, conn, if_exists='append', schema='public', index=True, index_label='Paikallinen_tunniste')
            except Exception as e:
                logger.error(f"Error occurred: {e}")
                failed_features_count += len(geom_gdf)

    return failed_features_count

def update_single_table_indexes(table_name, connection):
    """
    Updates indexes for a single table.

    Parameters:
    table_name (str): The name of the table to update indexes for.
    """
    logger.debug(f"Updating indexes for table: {table_name}")

    index_creation_sql = text(f'''
        CREATE INDEX IF NOT EXISTS "idx_{table_name}_Kunta" ON "{table_name}" ("Kunta");
        CREATE INDEX IF NOT EXISTS "idx_{table_name}_geom" ON "{table_name}" USING GIST (geometry);
    ''')
    connection.execute(index_creation_sql)
    connection.commit()

def update_indexes(table_names, use_multiprocessing=True):
    """
    Updates spatial and normal indexes for the given tables.

    Parameters:
    table_names (list): A list of PostGIS table names to update indexes for.
    use_multiprocessing (bool): Whether to use multiprocessing for updating indexes.
    """
    if table_names:
        with get_engine().connect() as connection:
            if use_multiprocessing:
                with concurrent.futures.ProcessPoolExecutor() as executor:
                    executor.map(update_single_table_indexes, table_names, [connection]*len(table_names))
            else:
                for table_name in table_names:
                    update_single_table_indexes(table_name, connection)
    else:
        logger.warning("No table names given, can't update table indexes")

def remove_duplicates(table_names):
    """
    Remove duplicate rows from the specified tables based on the 'Havainnon_tunniste' attribute to ensure DB has no occurrences with identical IDs.

    Parameters:
    table_names (list): The names of the tables to be checked for duplicates.

    Returns:
    int: The number of duplicate rows removed.
    """
    removed_occurrences = 0
    
    with get_engine().connect() as connection:
        # Add id column to all tables that need it (once, outside the loop)
        for table_name in table_names:
            id_column_check = connection.execute(text(f'''
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{table_name}' AND column_name = 'id';
            ''')).fetchone()

            if not id_column_check:
                connection.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN "id" SERIAL PRIMARY KEY;'))
        
        connection.commit()
        
        # Remove duplicates from each table
        for table_name in table_names:
            number_before_deletion = get_amount_of_occurrences(table_name)
            
            # Create new table with distinct rows, keeping the one with latest Lataus_pvm
            connection.execute(text(f'''
                CREATE TABLE "{table_name}_temp" AS
                SELECT DISTINCT ON ("Havainnon_tunniste") *
                FROM "{table_name}"
                ORDER BY "Havainnon_tunniste", "Lataus_pvm" DESC;
            '''))
            
            # Replace original table with deduplicated one
            connection.execute(text(f'DROP TABLE "{table_name}";'))
            connection.execute(text(f'ALTER TABLE "{table_name}_temp" RENAME TO "{table_name}";'))
            connection.commit()
            
            number_after_deletion = get_amount_of_occurrences(table_name)
            removed_occurrences += number_before_deletion - number_after_deletion

    return removed_occurrences

def merge_similar_observations(table_names, lookup_df):
    """
    Merge similar observations in PostGIS tables based on specified subset of columns and geometry.

    Parameters:
    table_names (list): List of PostGIS table names to process.
    lookup_df (DataFrame): DataFrame containing column configuration with 'groupby' and 'virva' columns.

    Returns:
    int: Total number of merged occurrences across all tables.
    """
    # Get columns to group by from lookup table
    columns_to_group_by = lookup_df.loc[lookup_df['merge_option'] == 'GROUPBY', 'virva'].values.tolist()
    columns_to_aggregate = lookup_df.loc[lookup_df['merge_option'] == 'AGGREGATE', 'virva'].values.tolist()
    columns_to_use_first_value = lookup_df.loc[lookup_df['merge_option'] == 'FIRST', 'virva'].values.tolist()
    columns_to_sum = lookup_df.loc[lookup_df['merge_option'] == 'SUM', 'virva'].values.tolist()
    columns_to_use_max = lookup_df.loc[lookup_df['merge_option'] == 'MAX', 'virva'].values.tolist()
    
    total_merged = 0
    
    with get_engine().connect() as connection:
        for table_name in table_names:
            if not check_table_exists(table_name):
                logger.error(f"Table {table_name} does not exist, skipping merging.")
                continue
                
            # Get count before merging
            count_before = get_amount_of_occurrences(table_name)
        
            # Create the groupby clause
            groupby_columns = ', '.join([f'"{col}"' for col in columns_to_group_by])
            
            # Add all columns
            agg_clauses = []
            for col in columns_to_use_first_value:
                agg_clauses.append(f'(ARRAY_AGG("{col}"))[1] as "{col}"')
            for col in columns_to_aggregate:
                agg_clauses.append(f'string_agg("{col}", \', \') FILTER (WHERE "{col}" IS NOT NULL AND "{col}" != \'nan\') as "{col}"')
            for col in columns_to_sum:
                agg_clauses.append(f'SUM("{col}") as "{col}"')
            for col in columns_to_use_max:
                agg_clauses.append(f'MAX("{col}") as "{col}"')
            
            # Include geometry and Yhdistetty
            agg_clauses.append('ST_SetSRID((ARRAY_AGG(geometry))[1],4326)::geometry(GEOMETRY,4326) AS geometry')
            agg_clauses.append('1 as "Yhdistetty"')
                       
            # Combine all aggregation clauses and build aggregation SQL
            all_agg_columns = ', '.join(agg_clauses)
            agg_sql = f'''
                CREATE TABLE merged_{table_name} AS
                SELECT 
                    {groupby_columns},
                    {all_agg_columns}
                FROM "{table_name}"
                GROUP BY {groupby_columns}
            '''

            # Execute the aggregations
            connection.execute(text(agg_sql))
            
            connection.execute(text(f'''
                UPDATE merged_{table_name} 
                SET "Yhdistetty" = array_length(string_to_array("Havainnon_tunniste", ', '), 1)
                WHERE "Havainnon_tunniste" LIKE '%,%'
            '''))

            connection.commit()
            
            # Replace the original table with merged data
            drop_table([table_name])
            connection.execute(text(f'ALTER TABLE merged_{table_name} RENAME TO "{table_name}"'))
            connection.commit()
                        
            # Get count after merging
            count_after = get_amount_of_occurrences(table_name)
            merged_count = count_before - count_after
            total_merged += merged_count
                
    return total_merged
