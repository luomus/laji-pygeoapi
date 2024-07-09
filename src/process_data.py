import pandas as pd
import geopandas as gpd
import re
from shapely.geometry import Polygon, MultiPolygon, GeometryCollection, Point, LineString, MultiPoint, MultiLineString
from shapely.ops import unary_union

def merge_taxonomy_data(occurrence_gdf, taxonomy_df):
    """
    Merge taxonomy information to the occurrence data.

    Parameters:
    occurrence_gdf (geopandas.GeoDataFrame): The occurrence data GeoDataFrame.
    taxonomy_df (pandas.DataFrame): The taxonomy data DataFrame.

    Returns:
    geopandas.GeoDataFrame: The merged GeoDataFrame.
    """
    occurrence_gdf['unit.linkings.originalTaxon.informalTaxonGroups[0]'] = occurrence_gdf['unit.linkings.originalTaxon.informalTaxonGroups[0]'].str.extract('(MVL\.\d+)')
    merged_gdf = occurrence_gdf.merge(taxonomy_df, left_on='unit.linkings.originalTaxon.informalTaxonGroups[0]', right_on='id', how='left')
    return merged_gdf

def get_min_max_dates(gdf):
    """
    Finds the minimum and maximum event dates and returns them in RFC3339 format

    Parameters:
    gdf (geopandas.GeoDataFrame): The GeoDataFrame containing column named eventDateTimeDisplay (or Aika).

    Returns:
    first_date: the first datestamp of the GeoDataframe in RFC3399 format
    end_date: the last datestamp of the Geodataframe in RFC3399 format
    """
    # Filter out NaT (Not a Time) values
    try:
        start_dates = pd.to_datetime(gdf['Keruu_aloitus_pvm']).dropna()
        end_dates = pd.to_datetime(gdf['Keruu_lopetus_pvm']).dropna()
    except ValueError as e:
        print(e)

    # Get the first date in RFC3339 format
    if len(start_dates) > 0:
        first_date = str(start_dates.min().strftime('%Y-%m-%dT%H:%M:%SZ'))
    else:
        first_date = None

    # Get the last date in RFC3399 format
    if len(end_dates) > 0:
        end_date = str(end_dates.max().strftime('%Y-%m-%dT%H:%M:%SZ'))
    else:
        end_date = None

    return first_date, end_date
    
def combine_similar_columns(gdf):
    """
    Finds similar columns (e.g. keyword[0], keyword[1], keyword[2]) and combines them

    Parameters:
    gdf (geopandas.GeoDataFrame): The GeoDataFrame similar columns with their names ending with [n]

    Returns:
    gdf (geopandas.GeoDataFrame): The geodataframe with combined columns
    """
    # Use regex to find columns with a pattern ending with [n]
    pattern = re.compile(r'^(.*)\[\d+\]$')
    
    # Dictionary to store the groups of columns
    columns_dict = {}

    for col in gdf.columns:
        match = pattern.match(col)
        if match:
            base_name = match.group(1)
            if base_name not in columns_dict:
                columns_dict[base_name] = []
            columns_dict[base_name].append(col)
    
    # Combine columns in each group
    for base_name, cols in columns_dict.items():
        gdf[base_name] = gdf[cols].apply(lambda row: ', '.join(row.dropna()), axis=1)
        gdf.drop(columns=cols, inplace=True)
    
    return gdf

def translate_column_names(gdf, lookup_table, style='virva'):
    """
    Maps column names in a GeoDataFrame to Finnish names and correct data types using a lookup table.

    Parameters:
    gdf (geopandas.GeoDataFrame): The GeoDataFrame to be mapped.
    lookup_table (str): The path to the CSV lookup table.
    style (str): Format to column names translate to. Options: 'translated_var', 'dwc' and 'virva'. Defaults to 'virva'.

    Returns:
    geopandas.GeoDataFrame: The GeoDataFrame with columns renamed and converted according to the lookup table.
    """
    # Load the lookup table CSV into a DataFrame
    lookup_df = pd.read_csv(lookup_table, sep=';', header=0)

    # Dictionary to hold the mapping of old column names to new column names and types
    column_mapping = {}
    column_types = {}
    columns_to_keep = []

    for column in lookup_df['finbif_api_var']:
        new_column_name = lookup_df.loc[lookup_df['finbif_api_var'] == column, style].iloc[0]
        new_column_type = lookup_df.loc[lookup_df['finbif_api_var'] == column, 'type'].iloc[0]
        column_types[new_column_name] = new_column_type
        columns_to_keep.append(new_column_name)

        if column in gdf.columns:
            column_mapping[column] = new_column_name
        else:
            gdf[new_column_name] = None

    # Rename existing columns
    gdf = gdf.rename(columns=column_mapping)

    # Drop columns that are not in the lookup table
    gdf = gdf[columns_to_keep]

    # Change column types according to the table and fill NaNs for integer columns
    for new_column_name, new_column_type in column_types.items():
        if new_column_type == 'int':
            gdf.fillna({new_column_name: 0}, inplace=True)
        if new_column_name != 'geometry':
            gdf[new_column_name] = gdf[new_column_name].astype(new_column_type)

    return gdf

def clean_table_name(group_name):
    """
    Cleans and formats a table name so that they can be used in PostGIS database.

    Parameters:
    group_name (str): The column name to be cleaned.

    Returns:
    str: The cleaned table name ready for PostGIS.
    """
    # Function to clean and return a table name
    if group_name is None or group_name =='nan' or group_name == '':
        return 'unclassified'
    
    # Remove non-alphanumeric characters and white spaces
    cleaned_name = re.sub(r'[^\w\s]', '', str(group_name)).replace(' ', '_')   

    # Shorten the name
    if len(cleaned_name) > 40:
        cleaned_name = cleaned_name[:40]

    return f'{cleaned_name}'

def convert_geometry_collection_to_multipolygon(geometry, buffer_distance=0.5):
    """Convert GeometryCollection to MultiPolygon, buffering points and lines if necessary."""
    if isinstance(geometry, GeometryCollection):
        polygons = []

        # Extract Point, LineString, Polygon, MultiPoint, MultiLineString, and MultiPolygon geometries from GeometryCollections
        for geom in geometry.geoms:
            if isinstance(geom, (Polygon, MultiPolygon)):
                polygons.append(geom)
            elif isinstance(geom, (Point, LineString, MultiPoint, MultiLineString)):
                polygons.append(geom.buffer(buffer_distance))

        # Convert polygons to MultiPolygon
        if len(polygons) == 1:
            if isinstance(polygons[0], Polygon):
                return MultiPolygon(polygons) # Return Multipygon created from the only Polygon
            else:
                return polygons[0] # Return MultiPolygon
        elif len(polygons) > 1: 
            return MultiPolygon(polygons) # Return MultiPolygon created from multiple Polygons
        else:
            return None  # Return None if no valid geometries are found
    return geometry  # Return the original geometry if it is not a GeometryCollection

def merge_duplicates(gdf):
    """
    Merge duplicates in a GeoDataFrame based on specified subset of columns and geometry.

    Parameters:
    gdf (GeoDataFrame): The input GeoDataFrame.

    Returns:
    GeoDataFrame: A GeoDataFrame with duplicates merged and a 'Yhdistetty' column added.
    """
    # Columns to consider for duplicates
    columns_to_check = ['Keruu_aloitus_pvm', 'Keruu_lopetus_pvm', 'ETRS_TM35FIN_WKT', 'Havainnoijat', 'Taksonin_tunniste']

    #missing_columns = [col for col in columns_to_check if col not in gdf.columns]
    #if missing_columns:
    #    raise ValueError(f"Missing columns in the GeoDataFrame: {missing_columns}")
    
    # Define how each column should be aggregated
    aggregation_dict = {col: 'first' for col in gdf.columns if col not in ['Keruutapahtuman_tunniste', 'Havainnon_tunniste', 'Yksilomaara_tulkittu']} # Select the first value for almost all columns
    aggregation_dict['Keruutapahtuman_tunniste'] = lambda x: list(x) if len(x) > 1 else x.iloc[0] # Create a list of the values if more than 1 items
    aggregation_dict['Havainnon_tunniste'] = lambda x: list(x) if len(x) > 1 else x.iloc[0] # Create a list of the values if more than 1 items
    aggregation_dict['Yksilomaara_tulkittu'] = 'sum' # Sum 'Yksilomaara_tulkitut'

    # Group by the columns to check for duplicates
    grouped = gdf.groupby(columns_to_check).agg(aggregation_dict)

    # Reset index for clarity
    grouped = grouped.reset_index(drop=True)

    # Create 'Yhdistetty' column
    grouped['Yhdistetty'] = grouped['Havainnon_tunniste'].apply(lambda x: len(x) if isinstance(x, list) else 1)

    # Calculate merged features
    amount_of_merged_occurrences = len(gdf) - len(grouped)

    return gpd.GeoDataFrame(grouped, geometry='geometry', crs=gdf.crs), amount_of_merged_occurrences

def validate_geometry(geom):
    """
    Repairs invalid geometries.

    Parameters:
    geom (GeoSeries): GeoSeries with geometry information

    Returns:
    geom (GeoSeries): GeoSeries with valid geometries
    edited_features_count (int): number of fixed geometries
    """
    # Use make_valid to ensure all geometries are valid
    original_geometry = geom.copy()        
    geom = geom.make_valid()
    edited_features_count = (geom != original_geometry).sum()
    return geom, edited_features_count