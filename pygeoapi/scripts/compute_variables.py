import pandas as pd
import numpy as np
import geopandas as gpd
import logging
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def compute_individual_count(col):
    """
    Determine whether the column gets a value 'paikalla' or 'poissa'.
    Keeps None or NaN values as they are.

    Parameters:
    individual_count_col (pd.Series): Column containing individual counts.

    Returns:
    pd.Series: Series with 'paikalla', 'poissa', or original NaN/None based on the individual count.
    """
    return col.apply(
        lambda x: 'paikalla' if x > 0 else 'poissa' if x <= 0 else None
    )

def compute_collection_id(collection_id_col, collection_names):
    """
    Computes collection names from collection ids.

    Parameters:
    collection_id_col (pd.Series): Column containing collection ids.
    collection_names (dict): Dictionary containing collection IDs and corresponding names.

    Returns:
    pd.Series: Series with corresponding collection names.
    """
    # Get only the IDs without URLs (e.g., 'http://tun.fi/HR.3553' to 'HR.3553')
    ids = collection_id_col.str.split('/').str[-1]

    # Return mapped values
    return ids.map(collection_names)

def map_values(col, value_ranges):
    """
    Function to map the values if more than one value in a cell.

    Parameters:
    col (pd.Series): Column with multiple values to map.
    value_ranges (dict): Mapping dictionary.

    Returns:
    pd.Series: Series with mapped values as a string.
    """
    return col.str.split(', ').apply(lambda values: ', '.join([value_ranges.get(re.sub(r'http://[^/]+\.fi/', '', value), value) for value in values]))

def compute_areas(gdf, municipals_gdf):
    """
    Computes the municipalities and provinces for each row in the GeoDataFrame.

    Parameters:
    gdf (gpd.GeoDataFrame): GeoDataFrame with geometry and IDs.
    municipal_geojson (gpd.GeoDataFrame): GeoDataFrame containing municipal geometries and corresponding ELY areas and provinces.

    Returns:
    pd.Series: Series with municipalities for each row, separated by ',' if there are multiple areas.
    pd.Series: Series with ELY areas for each row, separated by ',' if there are multiple areas.
    """
    def dedup_join(values): 
        return ', '.join(dict.fromkeys(str(v) for v in values if pd.notna(v) and v)) # Remove duplicate ELY center areas and empty values, ensure all are strings
    
    def str_join(values):
        return ', '.join(str(v) for v in values if pd.notna(v)) # Make sure values are strings and join with ','

    assert gdf.crs == municipals_gdf.crs, "CRS mismatch: municipals_gdf must be pre-aligned to gdf CRS"

    # Perform spatial join to find which areas each row is within
    joined_gdf = gpd.sjoin(gdf, municipals_gdf, how="left", predicate="intersects")

    # Group by the original index to aggregate municipalities and ELY areas
    municipalities = joined_gdf.groupby(joined_gdf.index)['Municipal_Name'].agg(str_join)
    elys = joined_gdf.groupby(joined_gdf.index)['ELY_Area_Name'].agg(dedup_join)

    municipalities = municipalities.reindex(gdf.index, fill_value='')
    elys = elys.reindex(gdf.index, fill_value='')

    return municipalities, elys

def get_title_name_from_table_name(table_name):
    """
    Converts table names back to the title names. E.g., 'sompion_lappi_polygons' -> 'Sompion Lappi'.

    Parameters:
    table_name (str): A PostGIS table name.

    Returns:
    str: A cleaned version of a PostGIS table name.
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
    return table_mapping.get(base_name, "Finland")

def get_biogeographical_region_from_id(id):
    """
    Converts biogeographical area id to the corresponding name. E.g., "ML.251" to "Ahvenanmaa".

    Parameters:
    id (str): Biogeographical area id.

    Returns:
    str: Corresponding name.
    """    
    name = id_mapping.get(id, "Empty biogeographical region")
    return name.replace(' ', '_').replace('-', '_').replace('ä', 'a').replace('ö', 'o').lower()  # Clean table name

def process_strip_url_columns(gdf, value_ranges):
    """
    Processes columns that require URL stripping before mapping.

    Returns:
    dict: Dictionary of processed columns.
    """
    columns = [
        'unit.atlasClass',
        'unit.atlasCode',
        'unit.linkings.taxon.primaryHabitat.habitat',
        'unit.linkings.taxon.latestRedListStatusFinland.status',
        'unit.linkings.taxon.threatenedStatus',
    ]

    result = {}
    for col in columns:
        if col in gdf.columns:
            result[col] = gdf[col].str.replace(r'http://[^/]+\.fi/', "", regex=True).map(value_ranges)
    return result

def process_direct_map_columns(gdf, value_ranges):
    """
    Processes columns that can be directly mapped.

    Returns:
    dict: Dictionary of processed columns.
    """
    # Columns without URL stripping
    columns = [
        'unit.recordBasis',
        'unit.interpretations.recordQuality',
        'document.secureReasons',
        'unit.lifeStage',
        'unit.sex',
        'unit.abundanceUnit',
        'document.linkings.collectionQuality',
    ]

    result = {}
    for col in columns:
        if col in gdf.columns:
            result[col] = gdf[col].map(value_ranges)
    return result

def compute_all(gdf, value_ranges, collection_names, municipals_gdf):
    """
    Computes or translates variables that cannot be directly accessed from the source API.

    Parameters:
    gdf (gpd.GeoDataFrame): GeoDataFrame containing occurrences.
    value_ranges (dict): Dictionary containing all mapping keys and corresponding values.
    collection_names (dict): Dictionary containing all collection IDs and their long names.
    municipal_geojson_path (str): Path to the GeoJSON file containing municipal geometries.

    Returns:
    gpd.GeoDataFrame: GeoDataFrame containing occurrences and computed columns.
    """
    # Create a dictionary to store all new values
    all_cols = {}

    all_cols.update(process_strip_url_columns(gdf, value_ranges))
    all_cols.update(process_direct_map_columns(gdf, value_ranges))

    # Mappings with multiple value in a cell:
    if 'unit.linkings.taxon.administrativeStatuses' in gdf.columns:
        all_cols['unit.linkings.taxon.administrativeStatuses'] = map_values(gdf['unit.linkings.taxon.administrativeStatuses'],value_ranges)

    # Computed values from different source
    all_cols['Esiintyman_tila'] = compute_individual_count(gdf['unit.interpretations.individualCount']) 
    all_cols['Aineisto'] = compute_collection_id(gdf['document.collectionId'], collection_names) 

    all_cols['Kunta'], all_cols['Vastuualue'] = compute_areas(gdf[['unit.unitId', 'geometry']], municipals_gdf)

    # Create a DataFrame to join
    computed_cols_df = pd.DataFrame(all_cols, dtype="str")

    # Drop duplicate columns
    gdf.drop(columns=computed_cols_df.columns.intersection(gdf.columns), axis=1, inplace=True)

    # Concatenate computed columns to gdf
    gdf = pd.concat([gdf, computed_cols_df], axis=1)

    # Create a local id
    gdf['Paikallinen_tunniste'] = gdf['unit.unitId'].str.replace("#", "_")
    return gdf