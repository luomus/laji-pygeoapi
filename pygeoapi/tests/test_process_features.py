from scripts import process_features
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from unittest.mock import patch

# run with:
# cd pygeoapi
# python -m pytest tests/test_process_features.py::test_process_json_features -q


class Dummy:
    # minimal attributes accessed by process_json_features
    taxon_df = None
    all_value_ranges = None
    collection_names = None
    municipals_gdf = None
    lookup_df = None


@patch('scripts.process_features.compute_all', side_effect=lambda gdf, *_, **__: gdf)
@patch('scripts.process_features.merge_taxonomy_data', side_effect=lambda gdf, *_: gdf)
def test_process_json_features(mock_merge, mock_compute):
    data = {
        'features': [
            {
                'type': 'Feature',
                'geometry': {'type': 'Point', 'coordinates': [24.94, 60.17]},
                'properties': {'unit.unitId': 'ID123', 'name': 'test'}
            }
        ]
    }

    # minimal lookup table needed by translate_column_names to keep geometry + id
    lookup_df = pd.DataFrame([
        {'finbif_api_var': 'unit.unitId', 'virva': 'Havainnon_tunniste', 'type': 'str'},
        {'finbif_api_var': 'geometry', 'virva': 'geometry', 'type': 'geom'}
    ])

    inst = Dummy()
    inst.lookup_df = lookup_df

    # run
    features = process_features.process_json_features(inst, data)

    # basic assertions
    assert isinstance(features, list)
    assert len(features) == 1
    f = features[0]
    assert f['type'] == 'Feature'
    assert f['properties']['Havainnon_tunniste'] == 'ID123'
    assert f['geometry']['type'] == 'Point'

    # call count assertions
    assert mock_merge.call_count == 1
    assert mock_compute.call_count == 1
