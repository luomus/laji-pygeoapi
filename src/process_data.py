import pandas as pd
import geopandas as gpd
import numpy as np
import re
from shapely.geometry import Polygon, MultiPolygon, GeometryCollection, Point, LineString, MultiPoint, MultiLineString
from shapely.ops import unary_union
import datetime

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

    # Dictionary to hold the mapping of old column names to new column names and types
    column_mapping = {}
    column_types = {}
    columns_to_keep = []
    new_columns_df = pd.DataFrame(index=gdf.index)

    for column in lookup_df['finbif_api_var']:
        new_column_name = lookup_df.loc[lookup_df['finbif_api_var'] == column, style].iloc[0]
        new_column_type = lookup_df.loc[lookup_df['finbif_api_var'] == column, 'type'].iloc[0]
        column_types[new_column_name] = new_column_type
        columns_to_keep.append(new_column_name)

        if column in gdf.columns:
            column_mapping[column] = new_column_name
        else:
            new_columns_df[new_column_name] = None
            print(f"Column {new_column_name} was not found so it is created with empty values")

    # Rename existing columns
    gdf = gdf.rename(columns=column_mapping)

    # Concatenate the new columns DataFrame with the original DataFrame
    gdf = pd.concat([gdf, new_columns_df], axis=1)

    # Drop columns that are not in the lookup table
    gdf = gdf[columns_to_keep]

    # Change column types according to the table and fill NaNs for integer columns
    for new_column_name, new_column_type in column_types.items():
        if new_column_type == 'int':
            gdf.fillna({new_column_name: 0}, inplace=True)
        if new_column_type == 'datetime':
            gdf[new_column_name] = pd.to_datetime(gdf[new_column_name], errors='coerce', format='%Y-%m-%d')
        if new_column_type != 'geom' and new_column_type != 'datetime':
            gdf[new_column_name] = gdf[new_column_name].astype(new_column_type)

    return gdf

def convert_geometry_collection_to_multipolygon(gdf, buffer_distance=0.5):
    """Convert GeometryCollection to MultiPolygon in the entire GeoDataFrame, buffering points and lines if necessary."""

    # Iterate over each row in the GeoDataFrame
    for idx in gdf.index:
        geometry = gdf.at[idx, 'geometry']
        
        if isinstance(geometry, GeometryCollection):
            print("Geometry collection found")
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
                    gdf.at[idx, 'geometry'] = MultiPolygon(polygons)  # Convert single Polygon to MultiPolygon
                else:
                    gdf.at[idx, 'geometry'] = polygons[0]  # Keep the MultiPolygon as is
            elif len(polygons) > 1:
                gdf.at[idx, 'geometry'] = MultiPolygon(polygons)  # Create MultiPolygon from multiple Polygons
            else:
                gdf.at[idx, 'geometry'] = None  # Set to None if no valid geometries are found

    return gdf

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