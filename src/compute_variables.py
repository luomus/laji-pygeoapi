import pandas as pd
import numpy as np
import geopandas as gpd

def compute_individual_count(individual_count_col):
    """
    Determine whether the column gets a value 'paikalla' or 'poissa'.
    Keeps None or NaN values as they are.

    Parameters:
    individual_count_col (pd.Series): Column containing individual counts.

    Returns:
    pd.Series: Series with 'paikalla', 'poissa', or original NaN/None based on the individual count.
    """
    return np.where(individual_count_col.isna(), individual_count_col, 
                    np.where(individual_count_col > 0, 'paikalla', 'poissa'))

def compute_collection_id(collection_id_col, collection_names):
    """
    Computes collection names from collection ids

    Parameters:
    collection_id_col (Pandas.Series): Column to contain collection ids
    collection_names (Dict): Dictionary derived from a json containing collection IDs and corresponding names

    Returns:
    ids (Pandas.Series): Corresponding collection names
    """
    # Get only the IDs without URLs (e.g. 'http://tun.fi/HR.3553' to 'HR.3553')
    ids = pd.Series(collection_id_col.str.split('/').str[-1])

    # Return mapped values
    return ids.map(collection_names)

def map_values(col, value_ranges):
    """
    Function to map the values if more than 1 value in a cell

    Parameters:
    col (Series): Column with multiple values to map
    value_ranges (Dictionary): Mapping dictionary

    Returns:
    (str) Mapped values as a string
    """

    return col.str.split(', ').apply(lambda values: ', '.join([value_ranges.get(value.strip("http://tun.fi/"), value) for value in values]))

def compute_areas(gdf_with_geom_and_ids, municipal_geojson):
    """
    Computes the municipalities and provinces for each row in the GeoDataFrame.

    Parameters:
    gdf_with_geom_and_ids (geopandas.GeoDataFrame): GeoDataFrame with geometry and IDs.
    municipal_geojson (str): Path to the GeoJSON file containing municipal geometries and corresponding ELY areas and provinces.

    Returns:
    pandas.Series: Series with municipalities for each row, separated by ',' if there are multiple areas.
    pandas.Series: Series with ely areas for each row, separated by ',' if there are multiple areas.
    """
    # Read the GeoJSON data
    municipal_gdf = gpd.read_file(municipal_geojson)

    # Ensure both GeoDataFrames use the same coordinate reference system (CRS)
    if gdf_with_geom_and_ids.crs != municipal_gdf.crs:
        municipal_gdf = municipal_gdf.to_crs(gdf_with_geom_and_ids.crs)

    # Perform spatial join to find which areas each row is within
    joined_gdf = gpd.sjoin(gdf_with_geom_and_ids, municipal_gdf, how="left", predicate="intersects")
    joined_gdf[['Municipal_Name', 'ELY_Area_Name']] = joined_gdf[['Municipal_Name', 'ELY_Area_Name']].fillna(value='')

    # Group by the original indices and aggregate the area names
    municipalities = joined_gdf.groupby(joined_gdf.index)['Municipal_Name'].agg(', '.join)
    elys = joined_gdf.groupby(joined_gdf.index)['ELY_Area_Name'].agg(', '.join)

    # Ensure the resulting Series aligns with the original GeoDataFrame's indices
    municipalities = municipalities.reindex(gdf_with_geom_and_ids.index, fill_value='')
    elys = elys.reindex(gdf_with_geom_and_ids.index, fill_value='')

    return municipalities, elys

def get_title_name_from_table_name(table_name):
    """
    Converts table names back to the title names. E.g. 'sompion_lappi_polygons' -> 'Sompion Lappi'

    Parameters: 
    table_name (str): A PostGIS table name

    Returns: 
    cleaned_valuea (str): A cleaned version of a PostGIS table name
    """
    # Define a dictionary to map table names to cleaned values
    table_mapping = {
        "sompion_lappi": "Sompion Lappi",
        "satakunta": "Satakunta",
        "pohjois_savo": "Pohjois-Savo",
        "pera_pohjanmaa": "Perä-Pohjanmaa",
        "laatokan_karjala": "Laatokan Karjala",
        "kittilan_lappi": "Kittilän Lappi",
        "keski_pohjanmaa": "Keski-Pohjanmaa",
        "kainuu": "Kainuu",
        "etela_hame": "Etelä-Häme",
        "enontekion_lappi": "Enontekiön Lappi",
        "ahvenanmaa": "Ahvenanmaa",
        "etela_savo": "Etelä-Savo",
        "etela_karjala": "Etelä-Karjala",
        "varsinais_suomi": "Varsinais-Suomi",
        "pohjois_hame": "Pohjois-Häme",
        "koillismaa": "Koillismaa",
        "uusimaa": "Uusimaa",
        "oulun_pohjanmaa": "Oulun Pohjanmaa",
        "inarin_lappi": "Inarin Lappi",
        "etela_pohjanmaa": "Etelä-Pohjanmaa",
        "pohjois_karjala": "Pohjois-Karjala"
    }

    # Remove the data type (e.g., points, polygons, lines)
    base_name = table_name.rsplit('_', 1)[0]
    
    # Look up the cleaned value in the dictionary
    cleaned_value = table_mapping.get(base_name, "Finland")
    
    return cleaned_value

def get_biogeographical_region_from_id(id):
    """
    This function converts biogeographical area id to the corresponding name. E.g. "ML.251" to "Ahvenanmaa"
    """
    id_mapping = {
        "ML.251": "Ahvenanmaa",
        "ML.252": "Varsinais-Suomi",
        "ML.253": "Uusimaa",
        "ML.254": "Etelä-Karjala",
        "ML.255": "Satakunta",
        "ML.256": "Etelä-Häme",
        "ML.257": "Etelä-Savo",
        "ML.258": "Laatokan Karjala",
        "ML.259": "Etelä-Pohjanmaa",
        "ML.260": "Pohjois-Häme",
        "ML.261": "Pohjois-Savo",
        "ML.262": "Pohjois-Karjala",
        "ML.263": "Keski-Pohjanmaa",
        "ML.264": "Kainuu",
        "ML.265": "Oulun Pohjanmaa",
        "ML.266": "Perä-Pohjanmaa",
        "ML.267": "Koillismaa",
        "ML.268": "Kittilän Lappi",
        "ML.269": "Sompion Lappi",
        "ML.270": "Enontekiön Lappi",
        "ML.271": "Inarin Lappi"
    }
    
    name = id_mapping.get(id, "Empty biogeographical region")
    cleaned_name = name.replace(' ', '_').replace('-', '_').replace('ä', 'a').replace('ö', 'o').lower() # Clean table name

    return cleaned_name

def compute_all(gdf, value_ranges, collection_names, municipal_geojson_path):
    '''
    Computes or translates variables that can not be directly accessed from the source API
   
    Parameters:
    gdf (geopandas.GeoDataFrame): GeoDataFrame containing occurrences.
    value_ranges (dict): The dictionary containig all mapping keys and corresponding values
    collection_names (dict): The dictionary containing all collection IDs and their long names
    municipal_geojson_path (str): Path to the GeoJSON file containing municipal geometries.

    Returns:
    gdf (geopandas.GeoDataFrame): GeoDataFrame containing occurrences and computed columns.
    '''
    # Create a dictionary to store all new values
    all_cols = {}

    # Columns that need URL stripping
    strip_url_columns = [
        'unit.atlasClass',
        'unit.atlasCode',
        'unit.linkings.originalTaxon.primaryHabitat.habitat',
        'unit.linkings.originalTaxon.latestRedListStatusFinland.status',
        'unit.linkings.taxon.threatenedStatus',
    ]

    # Columns without URL stripping
    direct_map_columns = [
        'unit.recordBasis',
        'unit.interpretations.recordQuality',
        'document.secureReasons',
        'unit.lifeStage',
        'unit.sex',
        'unit.abundanceUnit',
        'document.linkings.collectionQuality',
    ]

    # Process columns that require stripping before mapping
    for col in strip_url_columns:
        if col in gdf.columns:
            all_cols[col] = gdf[col].str.strip("http://tun.fi/").map(value_ranges)

    # Process columns that can be directly mapped
    for col in direct_map_columns:
        if col in gdf.columns:
            all_cols[col] = gdf[col].map(value_ranges)

    # Mappings with multiple value in a cell:
    all_cols['unit.linkings.originalTaxon.administrativeStatuses'] = map_values(gdf['unit.linkings.originalTaxon.administrativeStatuses'],value_ranges)


    # Computed values from different source
    all_cols['compute_from_individual_count'] = compute_individual_count(gdf['unit.interpretations.individualCount']) 
    all_cols['compute_from_collection_id'] = compute_collection_id(gdf['document.collectionId'], collection_names) 

    municipal_col, vastuualue_col = compute_areas(gdf[['unit.unitId', 'geometry']], municipal_geojson_path)
    all_cols['computed_municipality'] = municipal_col.astype('str')
    all_cols['computed_ely_area'] = vastuualue_col.astype('str')

    # Create a dataframe to join
    computed_cols_df = pd.DataFrame(all_cols, dtype="str")

    # Drop duplicate columns
    gdf.drop(columns=computed_cols_df.columns.intersection(gdf.columns), axis=1, inplace=True)

    # Concatenate computed columns to gdf
    gdf = pd.concat([gdf, computed_cols_df], axis=1)

    # Create a local id
    gdf['Paikallinen_tunniste'] = gdf['unit.unitId'].str.replace("http://tun.fi/", "").str.replace("#","_")

    return gdf