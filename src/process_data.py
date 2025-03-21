import pandas as pd
import geopandas as gpd
import numpy as np
import re
from shapely.geometry import Polygon, MultiPolygon, GeometryCollection, Point, LineString, MultiPoint, MultiLineString

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

def validate_geometry(gdf):
    """
    Repairs invalid geometries.
    Parameters:
    gdf (geopandas.GeoDataFrame): The occurrence data GeoDataFrame.

    Returns:
    gdf (geopandas.GeoDataFrame): The occurrence data GeoDataFrame where invalid geometries are fixed
    edited_features_count (int): number of fixed geometries
    """
    # Use make_valid to ensure all geometries are valid
    
    og_geom = gdf['geometry']
    gdf['geometry']  = gdf['geometry'].make_valid()
    edited_features_count = (gdf['geometry']  != og_geom).sum()
    return gdf, edited_features_count

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
    
    # DataFrame to store the combined columns
    combined_columns = pd.DataFrame(index=gdf.index)

    # Combine columns in each group
    for base_name, cols in columns_dict.items():
        combined_columns[base_name] = gdf[cols].apply(lambda row: ', '.join(row.dropna().astype(str)), axis=1)
        gdf.drop(columns=cols, inplace=True)

    # Concatenate the combined columns with the original DataFrame
    gdf = pd.concat([gdf, combined_columns], axis=1)

    return gdf

def translate_column_names(gdf, lookup_table, style='virva'):
    """
    Maps column names in a GeoDataFrame to Finnish names and correct data types using a lookup table.

    Parameters:
    gdf (geopandas.GeoDataFrame): The GeoDataFrame to be mapped.
    lookup_table (str): Path to the lookup table
    style (str): Format to column names translate to. Options: 'translated_var', 'dwc' and 'virva'. Defaults to 'virva'.

    Returns:
    geopandas.GeoDataFrame: The GeoDataFrame with columns renamed and converted according to the lookup table.
    """
    # Load the lookup table CSV into a DataFrame
    lookup_df = pd.read_csv(lookup_table, sep=';', header=0)

    # Create dictionaries for quick lookups
    column_mapping = lookup_df.set_index('finbif_api_var')[style].to_dict()
    column_types = lookup_df.set_index(style)['type'].to_dict()
    columns_to_keep = lookup_df[style].tolist()

    # Rename existing columns
    gdf = gdf.rename(columns=column_mapping)

    # Add missing columns with None values
    for col in columns_to_keep:
        if col not in gdf.columns:
            gdf[col] = None

    # Drop columns that are not in the lookup table
    gdf = gdf[columns_to_keep]

    # Change column types according to the table and fill NaNs for integer columns
    for col, col_type in column_types.items():
        if col_type == 'int':
            gdf[col] = gdf[col].astype(pd.Int64Dtype())
        elif col_type == 'datetime':
            gdf[col] = pd.to_datetime(gdf[col], errors='coerce', format='%Y-%m-%d')
        elif col_type == 'bool':
            gdf[col] = gdf[col].astype(str).str.lower().map({'true': True, 'false': False, 'none': None})
            gdf[col] = gdf[col].astype(pd.BooleanDtype())
        elif col_type != 'geom':
            gdf[col] = gdf[col].astype(col_type)

    # Convert all NaN values to None
    gdf = gdf.where(pd.notnull(gdf), None)

    return gdf

def convert_geometry_collection_to_multipolygon(gdf, buffer_distance=0.5):
    """Convert GeometryCollection to MultiPolygon in the entire GeoDataFrame, buffering points and lines if necessary.
       The resulting MultiPolygon is dissolved into a single geometry.
    """
    converted_collections = 0

    def process_geometry(geometry):
        nonlocal converted_collections
        if isinstance(geometry, GeometryCollection):
            polygons = [geom.buffer(buffer_distance) if isinstance(geom, (Point, LineString, MultiPoint, MultiLineString)) 
                        else geom 
                        for geom in geometry.geoms if isinstance(geom, (Polygon, MultiPolygon, Point, LineString, MultiPoint, MultiLineString))]

            if polygons:
                converted_collections += len(polygons)
                dissolved_geometry = gpd.GeoSeries(polygons).unary_union # dissolve to one multipolygon
                
                if isinstance(dissolved_geometry, Polygon):
                    dissolved_geometry = MultiPolygon([dissolved_geometry])

                return dissolved_geometry
            else:
                return None
        return geometry

    gdf['geometry'] = gdf['geometry'].apply(process_geometry)
    
    return gdf, converted_collections

def merge_duplicates(gdf, lookup_table):
    """
    Merge duplicates in a GeoDataFrame based on specified subset of columns and geometry.

    Parameters:
    gdf (GeoDataFrame): The input GeoDataFrame.
    lookup_table (str): Path to the lookup table.

    Returns:
    GeoDataFrame: A GeoDataFrame with duplicates merged and a 'Yhdistetty' column added.
    """

    # Load the lookup table CSV into a DataFrame
    lookup_df = pd.read_csv(lookup_table, sep=';', header=0)

    # Create a local id
    gdf['Paikallinen_tunniste'] = gdf['Havainnon_tunniste'].str.replace("http://tun.fi/", "").str.replace("#","_")

    # Convert the filtered DataFrame to a list of values
    columns_to_group_by = lookup_df.loc[lookup_df['groupby'] == True, 'virva'].values.tolist()

    # Define how each column should be aggregated
    aggregation_dict = {col: 'first' for col in gdf.columns if col not in ['Keruutapahtuman_tunniste', 'Havainnon_tunniste', 'Yksilomaara_tulkittu', 'Paikallinen_tunniste', 'Maara', 'Avainsanat', 'Havainnon_lisatiedot', 'Aineisto']} # Select the first value for almost all columns
    aggregation_dict['Keruutapahtuman_tunniste'] = lambda x: ', '.join(x) if len(x) > 1 else x.iloc[0] # Join values if more than 1 value
    aggregation_dict['Havainnon_tunniste'] = lambda x: ', '.join(x) if len(x) > 1 else x.iloc[0]
    aggregation_dict['Maara'] = lambda x: ', '.join(x) if len(x) > 1 else x.iloc[0]
    aggregation_dict['Avainsanat'] = lambda x: ', '.join(x) if len(x) > 1 else x.iloc[0]
    aggregation_dict['Havainnon_lisatiedot'] = lambda x: ', '.join(x) if len(x) > 1 else x.iloc[0]
    aggregation_dict['Aineisto'] = lambda x: ', '.join(x) if len(x) > 1 else x.iloc[0]
    aggregation_dict['Paikallinen_tunniste'] = lambda x: ', '.join(x) if len(x) > 1 else x.iloc[0]
    aggregation_dict['Yksilomaara_tulkittu'] = 'sum' # Sum values
 
    # Group by the columns to check for duplicates
    grouped = gdf.groupby(columns_to_group_by).agg(aggregation_dict).copy()

    # Reset index for clarity
    grouped = grouped.reset_index(drop=True)

    # Create 'Yhdistetty' column
    grouped['Yhdistetty'] = np.where(grouped['Havainnon_tunniste'].str.contains(','), grouped['Havainnon_tunniste'].str.count(',') + 1, 1)

    # Calculate merged features
    amount_of_merged_occurrences = len(gdf) - len(grouped)

    return gpd.GeoDataFrame(grouped, geometry='geometry', crs=gdf.crs), amount_of_merged_occurrences

def process_facts(gdf):
    """
    Convert unit.facts[n].fact and unit.facts[n].value columns into individual columns 
    in the GeoDataFrame, with the fact names as column names and corresponding values as column values.

    Parameters:
    gdf (geopandas.GeoDataFrame): The input GeoDataFrame.

    Returns:
    geopandas.GeoDataFrame: The processed GeoDataFrame with fact-value pairs converted to columns.
    """
    # Initialize a dictionary to hold the new columns
    new_columns = {}
    columns_to_add = ['Seurattava laji', 'Sijainnin tarkkuusluokka', 'Havainnon laatu', 'Peittävyysprosentti', 'Havainnon määrän yksikkö', 'Vesistöalue']


    # Identify all columns that match the pattern unit.facts[n].fact and unit.facts[n].value
    all_fact_colums = [col for col in gdf.columns if 'facts' in col]
    fact_columns = [col for col in gdf.columns if '].fact' in col]
    value_columns = [col for col in gdf.columns if '].value' in col]

    # Iterate over the fact columns
    for fact_col, value_col in zip(fact_columns, value_columns):

        # Iterate over each row in the GeoDataFrame
        for idx, fact_name in gdf[fact_col].items():
            if pd.notna(fact_name) and fact_name in columns_to_add:  # If the fact_name is not null and is in the list

                # If the fact_name column does not already exist in new_columns, initialize it
                if fact_name not in new_columns:
                    new_columns[fact_name] = [None] * len(gdf)
                
                # Assign the corresponding value to the new column
                new_columns[fact_name][idx] = gdf.at[idx, value_col]

    # Drop the original fact and value columns
    gdf.drop(columns=all_fact_colums, axis=1, inplace=True)

    # Convert the new_columns dictionary into a DataFrame
    new_columns_df = pd.DataFrame(new_columns, index=gdf.index)

    # Concatenate the new columns with the original GeoDataFrame
    gdf = pd.concat([gdf, new_columns_df], axis=1)

    return gdf