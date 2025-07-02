import unittest, sys
import geopandas as gpd
import pandas as pd
import os
from dotenv import load_dotenv
from unittest.mock import patch, MagicMock
import requests

sys.path.append('src/')

import load_data

# run with:
# python -m pytest tests/test_load_data.py -v

@patch('requests.get')
def test_fetch_json_with_retry(mock_get):
    # Test successful fetch
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"key": "value"}
    mock_get.return_value = mock_response
    result = load_data.fetch_json_with_retry("http://example.com/api")
    assert result == {"key": "value"}
    mock_get.assert_called_once_with("http://example.com/api")

    # Test retry on failure and then success
    mock_get.reset_mock()
    mock_get.side_effect = [requests.exceptions.RequestException, MagicMock(status_code=200, json=lambda: {"key": "value"})]
    result = load_data.fetch_json_with_retry("http://example.com/api", max_retries=3, delay=1)
    assert result == {"key": "value"}
    assert mock_get.call_count == 2

    # Test exceed max retries
    mock_get.reset_mock()
    mock_get.side_effect = requests.exceptions.RequestException
    result = load_data.fetch_json_with_retry("http://example.com/api", max_retries=3, delay=1)
    assert result is None
    assert mock_get.call_count == 3

@patch('load_data.fetch_json_with_retry')
def test_get_collection_names(mock_fetch):
    mock_fetch.return_value = {
        'results': [
            {'id': '1', 'longName': 'Collection One'},
            {'id': '2', 'longName': 'Collection Two'}
        ]
    }
    result = load_data.get_collection_names("http://example.com/api")
    expected = {'1': 'Collection One', '2': 'Collection Two'}
    assert result == expected
    mock_fetch.assert_called_once_with("http://example.com/api")

def test_get_last_page_valid():
    url = "https://beta.laji.fi/api/warehouse/query/unit/list?page=1&pageSize=1&geoJSON=true&featureType=ORIGINAL_FEATURE"
    last_page = load_data.get_last_page(url)
    assert isinstance(last_page, int)
    assert last_page >= 1

def test_download_page():
    url = "https://beta.laji.fi/api/warehouse/query/unit/list?page=1&pageSize=1&geoJSON=true&featureType=ORIGINAL_FEATURE"
    gdf = load_data.download_page(url, page_no=1)
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert not gdf.empty

def test_get_occurrence_data():
    url = "https://beta.laji.fi/api/warehouse/query/unit/list?page=1&pageSize=1&geoJSON=true&featureType=ORIGINAL_FEATURE&time=/-1"
    gdf, _ = load_data.get_occurrence_data(url=url, startpage=1, endpage=2, multiprocessing=True)
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert not gdf.empty
    assert gdf['geometry'].dtype == 'geometry'
    assert gdf['unit.unitId'].dtype == 'object'

    gdf2, _ = load_data.get_occurrence_data(url=url, startpage=1, endpage=2, multiprocessing=False)
    assert isinstance(gdf2, gpd.GeoDataFrame)
    assert not gdf2.empty
    assert gdf2['geometry'].dtype == 'geometry'
    assert gdf2['unit.unitId'].dtype == 'object'
    assert gdf.crs == gdf2.crs
    assert set(gdf.columns) == set(gdf2.columns)

    gdf_sorted = gdf.sort_values("unit.unitId").reset_index(drop=True)
    gdf2_sorted = gdf2.sort_values("unit.unitId").reset_index(drop=True)
    pd.testing.assert_frame_equal(gdf_sorted, gdf2_sorted, check_like=True)

def test_get_value_ranges():
    url = "https://beta.laji.fi/api/warehouse/query/unit/list?page=1&pageSize=1&geoJSON=true&featureType=ORIGINAL_FEATURE"
    result = load_data.get_value_ranges(url)
    assert isinstance(result, dict)
    assert 'results' in result
    assert isinstance(result['results'], list)

def test_get_taxon_data():
    taxon_name_url = f'https://beta.laji.fi/api/informal-taxon-groups?pageSize=1'
    df = load_data.get_taxon_data(taxon_name_url)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert 'id' in df.columns
    assert 'name' in df.columns

def test_get_enumerations():
    url = "https://beta.laji.fi/api/warehouse/enumeration-labels"
    result = load_data.get_enumerations(url)
    assert isinstance(result, dict)
    assert result['IMAGE'] == 'Kuva'