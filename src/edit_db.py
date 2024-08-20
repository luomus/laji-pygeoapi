from sqlalchemy import inspect
from sqlalchemy import create_engine, text, MetaData
import geopandas as gpd
import pandas as pd
import os
from dotenv import load_dotenv

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
    Retrieves all table names in the database.
    """
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    return tables

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

def to_db(gdf, table_names, failed_features_count, occurrences_without_group_count, last_iteration=False):
    """
    Process and insert geospatial data into a PostGIS database.

    Parameters:
    gdf (GeoDataFrame): The main GeoDataFrame containing occurrences.
    table_names (list): A list to store table names that have been processed.
    failed_features_count (int): A counter for failed occurrence inserts.
    occurrences_without_group_count (int): A counter for occurrences without a group
    last_iteration (bool): Flag indicating whether this is the last iteration.
    """
    
    # Explode gdf based on Vastuualue
    gdf['Vastuualue_list'] = gdf['Vastuualue'].str.split(', ') 
    gdf = gdf.explode(column='Vastuualue_list')

    # Process each unique group
    occurrences_without_group_count += gdf['Vastuualue_list'].isnull().sum()
    gdf = gdf.dropna(subset=['Vastuualue_list'])

    unique_groups = gdf['Vastuualue_list'].unique()
    for table_name in unique_groups:
        if table_name:
            # Filter the sub DataFrame
            sub_gdf = gdf[gdf['Vastuualue_list'] == table_name]
            sub_gdf = sub_gdf.drop('Vastuualue_list', axis=1)

            try:
                with engine.connect() as conn:
                    sub_gdf.to_postgis(table_name, conn, if_exists='append', schema='public', index=False)
                if table_name not in table_names:
                    table_names.append(table_name)
            except Exception as e:
                print(f"Error occurred: {e}")
                failed_features_count += len(sub_gdf)

            del sub_gdf

    return table_names, failed_features_count, occurrences_without_group_count

def update_indexes(table_name):
    """
    Updates spatial and normal indexes for the given table

    Parameters:
    table_name (str): A PostGIS table name to where indexes will be updated
    """

    with engine.connect() as connection:
        reindex_table = text(f'REINDEX TABLE "{table_name}";')
        connection.execute(reindex_table)

        reindex_id = text(f'CREATE INDEX "idx_{table_name}_Kunta" ON "{table_name}" ("Kunta");')
        connection.execute(reindex_id)

        reindex_id2 = text(f'CREATE INDEX "idx_{table_name}_Suomenkielinen_nimi" ON "{table_name}" ("Suomenkielinen_nimi");')
        connection.execute(reindex_id2)

        spatial_reindex_sql = text(f'CREATE INDEX "idx_{table_name}_geom" ON "{table_name}" USING GIST (geometry);')
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
        connection.commit()
    return edited_features_count

def collections_to_multis(table_name, buffer_distance=0.5):
    """
    Convert GeometryCollections to MultiPolygons with a specified buffer distance.

    Args:
        table_name (str): The name of the table containing geometry data.
        buffer_distance (float): The buffer distance to apply to non-polygon geometries.

    Returns:
        None
    """
    
    # This SQL converts GeometryCollections to MultiPolygons and counts modified features
    sql = f"""
        DO $$
        DECLARE
            rec RECORD;
            polygons GEOMETRY[];
            geom GEOMETRY;
            new_geom GEOMETRY;
            modified_count INTEGER := 0;
        BEGIN
            -- Iterate over each row in the table
            FOR rec IN SELECT "Paikallinen_tunniste", geometry FROM "{table_name}" LOOP
                -- Initialize an empty array for polygons
                polygons := ARRAY[]::GEOMETRY[];
                
                -- Check if the geometry is a GeometryCollection
                IF ST_GeometryType(rec.geometry) = 'ST_GeometryCollection' THEN
                    
                    -- Iterate over each geometry in the collection
                    FOR geom IN SELECT (ST_Dump(rec.geometry)).geom LOOP
                        -- Check the type of each geometry and process accordingly
                        CASE ST_GeometryType(geom)
                            WHEN 'ST_Polygon', 'ST_MultiPolygon' THEN
                                -- Add Polygon or MultiPolygon directly to the array
                                polygons := array_append(polygons, geom);
                            WHEN 'ST_Point', 'ST_MultiPoint', 'ST_LineString', 'ST_MultiLineString' THEN
                                -- Buffer Point, MultiPoint, LineString, and MultiLineString
                                polygons := array_append(polygons, ST_Buffer(geom, {buffer_distance}));
                        END CASE;
                    END LOOP;
                    
                    -- Create MultiPolygon from collected geometries if any valid polygons exist
                    IF array_length(polygons, 1) > 0 THEN
                        new_geom := ST_Multi(ST_Collect(polygons));
                        
                        -- Update the table with the new geometry
                        UPDATE "{table_name}"
                        SET geometry = new_geom
                        WHERE "Paikallinen_tunniste" = "rec.Paikallinen_tunniste";
                        
                        -- Increment the modified count
                        modified_count := modified_count + 1;
                    END IF;
                END IF;
            END LOOP;
            
            -- Output the number of modified features
            RAISE NOTICE 'Number of features modified: %', modified_count;
        END
        $$;
    """

    # Execute the SQL code block
    with engine.connect() as connection:
        connection.execute(text(sql))
        connection.commit()
        
        # Print out any notices (such as the modification count) from the SQL execution
        for notice in connection.connection.notices:
            print(notice.strip())

engine = connect_to_db()