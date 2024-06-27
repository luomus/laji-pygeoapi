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
    start_dates = pd.to_datetime(gdf['Keruu_aloitus_pvm']).dropna()
    end_dates = pd.to_datetime(gdf['Keruu_lopetus_pvm']).dropna()


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
    Maps column names in a GeoDataFrame to Darwin Core names using a lookup table.

    Parameters:
    gdf (geopandas.GeoDataFrame): The GeoDataFrame to be mapped.
    lookup_table (str): The path to the CSV lookup table.
    style (str): Format to column names tranlate to. Options: 'translated_var', 'dwc' and 'virva'. Defaults to 'virva'.

    Returns:
    geopandas.GeoDataFrame: The GeoDataFrame with columns renamed according to the lookup table.
    """
    # Load the lookup table CSV into a DataFrame
    lookup_df = pd.read_csv(lookup_table, sep=';', header=0)

    column_mapping = {}
    columns_to_remove = []

    # Iterate through the lookup table
    for _, row in lookup_df.iterrows():
        if len(str(row[style])) > 3:
            # Map columns if the given format exists
            column_mapping[row['finbif_api_var']] = row[style]
        else:
            # If there is nothing to map, add column to removable list
            columns_to_remove.append(row['finbif_api_var'])

    # Remove columns from gdf that do not have a corresponding variable in lookup_df
    gdf.drop(columns=columns_to_remove, inplace=True, errors='ignore')

    gdf = combine_similar_columns(gdf)

    # Rename columns based on the mapping
    gdf.rename(columns=column_mapping, inplace=True)

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
    columns_to_check = ['Keruu_aloitus_pvm', 'Keruu_lopetus_pvm', 'ETRS_TM35FIN_WKT', 'Havainnoijat', 'Taksonin_tunniste', 'geometry']

    # Define how each column should be aggregated
    aggregation_dict = {col: 'first' for col in gdf.columns if col not in ['Keruutapahtuman_tunniste', 'Havainnon_tunniste', 'Yksilomaara_tulkittu']} # Select the first value for almost all columns
    aggregation_dict['Keruutapahtuman_tunniste'] = lambda x: list(x) if len(x) > 1 else x.iloc[0] # Create a list of the values if more than 1 items
    aggregation_dict['Havainnon_tunniste'] = lambda x: list(x) if len(x) > 1 else x.iloc[0] # Create a list of the values if more than 1 items
    aggregation_dict['Yksilomaara_tulkittu'] = 'sum' # Sum the values

    # Group by the columns to check for duplicates
    grouped = gdf.groupby(columns_to_check).agg(aggregation_dict)

    # TODO: Add 'Yhdistetty' column

    # Reset index for clarity
    grouped = grouped.reset_index(drop=True)

    # Calculate merged features
    amount_of_merged_occurrences = len(gdf) - len(grouped)

    return gpd.GeoDataFrame(grouped, geometry='geometry', crs=gdf.crs), amount_of_merged_occurrences

