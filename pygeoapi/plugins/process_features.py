import logging
import geopandas as gpd
from compute_variables import compute_all
from process_data import merge_taxonomy_data, combine_similar_columns, translate_column_names, convert_geometry_collection_to_multipolygon, validate_geometry, process_facts

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_json_features(self, data, crs='EPSG:4326'):
    """
    Convert features to GeoDataFrame, update 'id' column, and convert back to GeoJSON FeatureCollection.
    """
    
    features = data.get('features', [])
   
    # update 'id' 
    for feature in features:
        unit_id = feature.get('properties', {}).get('unit.unitId', '')
        if unit_id:
            feature['properties']['id'] = unit_id.replace('#', '~').replace('http://tun.fi/', '')

    # Convert features to GeoDataFrame
    gdf = gpd.GeoDataFrame.from_features(features, crs=crs)

    # Process the GeoDataFrame to follow the same schema as the other data
    gdf = merge_taxonomy_data(gdf, self.taxon_df)
    gdf = process_facts(gdf)
    gdf = combine_similar_columns(gdf)
    gdf = compute_all(gdf, self.all_value_ranges, self.collection_names, self.municipals_gdf)
    gdf = translate_column_names(gdf, self.lookup_df, style='virva')
    gdf, _ = convert_geometry_collection_to_multipolygon(gdf)
    gdf, _ = validate_geometry(gdf)

    # Set index
    if 'Paikallinen_tunniste' in gdf.columns:
        gdf = gdf.set_index('Paikallinen_tunniste', drop=False)

    return gdf.__geo_interface__['features']

