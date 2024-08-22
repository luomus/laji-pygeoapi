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

    for column in lookup_df['finbif_api_var']:
        new_column_name = lookup_df.loc[lookup_df['finbif_api_var'] == column, style].iloc[0]
        new_column_type = lookup_df.loc[lookup_df['finbif_api_var'] == column, 'type'].iloc[0]
        column_types[new_column_name] = new_column_type
        columns_to_keep.append(new_column_name)

        if column in gdf.columns:
            column_mapping[column] = new_column_name
        else:
            gdf[new_column_name] = None
            print(f"Column {new_column_name} was not found so it is created with empty values")

    # Rename existing columns
    gdf = gdf.rename(columns=column_mapping)

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

def get_facts(gdf):
    # List of columns to be added to the DataFrame
    columns_to_add = ['Seurattava laji', 'Sijainnin tarkkuusluokka', 'Havainnon laatu', 'Peittävyysprosentti', 'Havainnon määrän yksikkö', 'Vesistöalue', 'Merialueen tunniste']
    new_columns = {column: [None] * len(gdf) for column in columns_to_add}

    # Process the facts
    fact_cols = gdf.filter(like='].fact').columns
    if len(fact_cols) > 0:
        columns_to_drop = list(fact_cols)
        for fact_col in fact_cols: # Loop over all facts
            value_col = fact_col.replace('].fact', '].value') # Get value columns from the fact column with the same id (e.g. gathering.facts[2].fact -> gathering.facts[2].value)
            columns_to_drop.append(value_col)
            if value_col in gdf.columns:
                facts = gdf[fact_col] 
                values = gdf[value_col]
                for fact_name in columns_to_add: 
                    mask = facts == fact_name # Create a mask
                    new_columns[fact_name] = values.where(mask, new_columns[fact_name])
        
        # Drop fact and value columns since all their values have been retrieved
        try:
            gdf.drop(columns=columns_to_drop, axis=1, inplace=True) 
        except KeyError as e:
            print("Cannot drop column..")
            print(e)

    # Add facts to the gdf as new columns
    new_columns_df = pd.DataFrame(new_columns, index=gdf.index, dtype='str')
    gdf = pd.concat([gdf, new_columns_df], axis=1)

    return gdf