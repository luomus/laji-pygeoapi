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
    occurrence_gdf['unit.linkings.originalTaxon.informalTaxonGroups[0]'] = occurrence_gdf['unit.linkings.originalTaxon.informalTaxonGroups[0]'].str.extract(r'(MVL\.\d+)')
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
    invalid = ~gdf['geometry'].is_valid
    gdf.loc[invalid, 'geometry'] = gdf.loc[invalid, 'geometry'].make_valid()
    edited_features_count = invalid.sum()
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
            columns_dict.setdefault(base_name, []).append(col)
    
    gdf = gdf.copy()

    # Combine columns in each group
    for base_name, cols in columns_dict.items():
        gdf[base_name] = gdf[cols].astype(str).where(gdf[cols].notna(), '').agg(', '.join, axis=1).str.strip(', ')
        gdf.drop(columns=cols, inplace=True)

    return gdf

def translate_column_names(gdf, lookup_df, style='virva'):
    """
    Maps column names in a GeoDataFrame to Finnish names and correct data types using a lookup table.

    Parameters:
    gdf (geopandas.GeoDataFrame): The GeoDataFrame to be mapped.
    lookup_df (pandas.DataFrame): A DataFrame containing the lookup table with columns 'finbif_api_var', 'translated_var', 'dwc', 'virva', and 'type'.
    style (str): Format to column names translate to. Options: 'translated_var', 'dwc' and 'virva'. Defaults to 'virva'.

    Returns:
    geopandas.GeoDataFrame: The GeoDataFrame with columns renamed and converted according to the lookup table.
    """ 

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
            converted_collections += 1
            geom_types = {type(geom) for geom in geometry.geoms}
            geometries = list(geometry.geoms)

            # If the GeometryCollection has only one geometry, return it as-is
            if len(geometries) == 1:
                return geometries[0]

            # If all geometries are of the same type, convert to MultiX
            if geom_types == {LineString}:
                return MultiLineString(list(geometry.geoms))
            elif geom_types == {Point}:
                return MultiPoint(list(geometry.geoms))
            elif geom_types == {Polygon}:
                return MultiPolygon(list(geometry.geoms))
            elif geom_types == {MultiLineString}:
                return MultiLineString([g for geom in geometry.geoms for g in geom.geoms])
            elif geom_types == {MultiPoint}:
                return MultiPoint([g for geom in geometry.geoms for g in geom.geoms])
            elif geom_types == {MultiPolygon}:
                return MultiPolygon([g for geom in geometry.geoms for g in geom.geoms])

            # In other case, buffer points and lines and return the dissolved result as MultiPolygon
            polygons = [geom.buffer(buffer_distance) if isinstance(geom, (Point, LineString, MultiPoint, MultiLineString)) 
                        else geom 
                        for geom in geometry.geoms if isinstance(geom, (Polygon, MultiPolygon, Point, LineString, MultiPoint, MultiLineString))]

            if polygons:
                dissolved_geometry = gpd.GeoSeries(polygons).union_all() 
                
                if isinstance(dissolved_geometry, Polygon):
                    return MultiPolygon([dissolved_geometry])

                return dissolved_geometry
            else:
                return None
        return geometry

    gdf['geometry'] = gdf['geometry'].apply(process_geometry)
    
    return gdf, converted_collections

def merge_duplicates(gdf, lookup_df):
    """
    Merge duplicates in a GeoDataFrame based on specified subset of columns and geometry.

    Parameters:
    gdf (GeoDataFrame): The input GeoDataFrame.
    lookup_table (str): Path to the lookup table.

    Returns:
    GeoDataFrame: A GeoDataFrame with duplicates merged and a 'Yhdistetty' column added.
    """
    def join_or_use_first(x):
        # Efficiently join only non-null, unique values
        values = pd.Series(x).dropna().astype(str).unique()
        return ', '.join(values) if len(values) > 1 else (values[0] if len(values) == 1 else None)

    # Create a local id
    gdf['Paikallinen_tunniste'] = gdf['Havainnon_tunniste'].str.replace("http://tun.fi/", "").str.replace("#","_")

    # Convert the filtered DataFrame to a list of values
    columns_to_group_by = lookup_df.loc[lookup_df['groupby'] == True, 'virva'].values.tolist()

    # Define how each column should be aggregated
    aggregation_dict = {}
    for col in gdf.columns:
        if col in ['Keruutapahtuman_tunniste', 'Havainnon_tunniste', 'Maara', 'Avainsanat', 'Havainnon_lisatiedot', 'Aineisto', 'Paikallinen_tunniste']:
            aggregation_dict[col] = join_or_use_first
        elif col == 'Yksilomaara_tulkittu':
            aggregation_dict[col] = 'sum'
        else:
            aggregation_dict[col] = 'first'
 
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
    columns_to_add = ['Seurattava laji', 'Sijainnin tarkkuusluokka', 'Havainnon laatu', 'Peittävyysprosentti', 'Havainnon määrän yksikkö', 'Vesistöalue']


    # Identify all columns that match the pattern unit.facts[n].fact and unit.facts[n].value
    all_fact_columns = [col for col in gdf.columns if 'facts' in col]
    fact_columns = [col for col in gdf.columns if '].fact' in col]
    value_columns = [col for col in gdf.columns if '].value' in col]

    # Prepare a dict for new columns
    new_columns = {col: pd.Series([None]*len(gdf), index=gdf.index) for col in columns_to_add}

    # For each fact/value column pair, fill the new columns
    for fact_col, value_col in zip(fact_columns, value_columns):
        facts = gdf[fact_col]
        values = gdf[value_col]
        for col in columns_to_add:
            mask = facts == col
            new_columns[col][mask] = values[mask]

    # Drop the original fact and value columns
    gdf = gdf.drop(columns=all_fact_columns, axis=1)

    # Add new columns to gdf
    for col in columns_to_add:
        gdf[col] = new_columns[col]

    return gdf