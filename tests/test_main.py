import sys
import unittest
from unittest.mock import patch, MagicMock
import os
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, GeometryCollection, Polygon


from pygeoapi.scripts import main

# run with:
# python -m pytest tests/test_main.py -v

@patch.dict(os.environ, {
    'ACCESS_TOKEN': 'test_access_token',
    'POSTGRES_DB': 'test_db',
    'POSTGRES_USER': 'test_user',
    'POSTGRES_PASSWORD': 'test_pw',
    'POSTGRES_HOST': 'test_host',
    'PAGES': '1',
    'MULTIPROCESSING': 'False',
    'RUNNING_IN_OPENSHIFT': 'False'
})
@patch('pygeoapi.scripts.main.load_dotenv')
def test_setup_environment(mock_load_dotenv):
    # Ensure the mock is used to avoid the "not accessed" warning
    mock_load_dotenv.assert_not_called()
    config = main.setup_environment()
    assert config['access_token'] == 'test_access_token'
    assert config['pygeoapi_config_out'] == r'pygeoapi-config.yml'
    assert config['metadata_db_path'] == 'catalogue.tinydb'


@patch('pygeoapi.scripts.main.edit_db.to_db', return_value=0)
@patch('pygeoapi.scripts.main.edit_db.remove_duplicates', return_value=0)
@patch('pygeoapi.scripts.main.load_data.get_occurrence_data')
@patch('pygeoapi.scripts.main.compute_variables.compute_all')
def test_load_and_process_data(mock_compute_all, mock_get_occurrence_data, mock_remove_duplicates, mock_to_db):
    gdf = gpd.GeoDataFrame({
        'unit.unitId': ['id1', 'id2', 'id3', 'id4'],
        'geometry': [
            Point(24.9384, 60.1699),
            GeometryCollection([Point(24.9384, 60.1699), Point(24.9385, 60.1700)]),
            Polygon([(0, 0), (2, 0), (0, 2), (2, 2), (0, 0)]),
            GeometryCollection([Point(25.1, 60.3)])
        ],
        'unit.linkings.originalTaxon.informalTaxonGroups[0]': ['MVL.1', 'MVL.2', 'MVL.1', 'MVL.2'],
        'unit.linkings.originalTaxon.administrativeStatuses': [['status1', 'status2'], ['status3'], ['status4'], ['status5']],
        'unit.interpretations.individualCount': [10, 20, 5, 15],
        'document.collectionId': ['HR.1', 'HR.2', 'HR.1', 'HR.2'],
        'unit.recordBasis': ['Observation', 'Specimen', 'Observation', 'Specimen'],
        'unit.interpretations.recordQuality': ['High', 'Medium', 'Low', 'Medium'],
        'gatherings.facts[0].fact': ['Fact1', 'Fact2', 'Fact1', 'Fact2'],
        'gatherings.facts[0].value': ['Value1', 'Value2', 'Value3', 'Value4'],
    }, geometry='geometry', crs="EPSG:4326")

    taxon_df = pd.DataFrame({'id': ['MVL.1', 'MVL.2'], 'name': ['Birds', 'Snakes']})
    collection_names = {'HR.1': 'Test Collection', 'HR.2': 'Another Collection'}
    all_value_ranges = {}
    config = {"multiprocessing": "False", "batch_size": 5}

    mock_get_occurrence_data.return_value = (gdf, 0) # Return GeoDataFrame and 0 errors
    mock_compute_all.side_effect = lambda gdf, *args, **kwargs: gdf # Mock compute_all to return the input GeoDataFrame

    lookup_df = pd.read_csv("pygeoapi/scripts/resources/lookup_table_columns.csv", sep=';', header=0)

    results = main.load_and_process_data(
        "occurrence_url", "uusimaa", 1, config, all_value_ranges, taxon_df, collection_names, "mock_path", lookup_df
    )
    assert results == (4, 0, 1, 0, 2) # 4 occurrences, 0 failed, 1 edited, 0 duplicates, 2 processed geometry collections
