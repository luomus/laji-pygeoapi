import geopandas as gpd
import pandas as pd
from unittest.mock import patch, MagicMock
import requests

from scripts import load_data
import time

# run with:
# cd pygeoapi
# python -m pytest tests/test_load_data.py -v

def test_is_cache_valid():
    
    # Test cache key that doesn't exist
    assert not load_data._is_cache_valid("nonexistent_key")
    
    # Test valid cache (recent timestamp)
    test_key = "test_key"
    load_data._cache_timestamps[test_key] = time.time()
    assert load_data._is_cache_valid(test_key)
    
    # Test expired cache (old timestamp)
    expired_key = "expired_key"
    load_data._cache_timestamps[expired_key] = time.time() - load_data._cache_timeout - 1
    assert not load_data._is_cache_valid(expired_key)
    
    # Test cache at the edge of expiration
    edge_key = "edge_key"
    load_data._cache_timestamps[edge_key] = time.time() - load_data._cache_timeout + 1
    assert load_data._is_cache_valid(edge_key)
    
    # Clean up test data
    load_data._cache_timestamps.pop(test_key, None)
    load_data._cache_timestamps.pop(expired_key, None)
    load_data._cache_timestamps.pop(edge_key, None)

@patch('scripts.load_data.get_enumerations')
@patch('scripts.load_data.get_value_ranges')
@patch('scripts.load_data.get_collection_names')
@patch('scripts.load_data.get_taxon_data')
@patch('scripts.load_data.get_municipality_ids')
@patch('pandas.read_csv')
@patch('pandas.read_json')
def test_load_or_update_cache(mock_read_json, mock_read_csv, mock_get_municipality_ids, 
                             mock_get_taxon_data, mock_get_collection_names, 
                             mock_get_value_ranges, mock_get_enumerations):
    # Setup mocks
    mock_gdf = MagicMock()
    mock_gdf.to_crs.return_value = mock_gdf
    mock_gdf.sindex = MagicMock()
    mock_read_json.return_value = mock_gdf
    
    mock_lookup_df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    mock_read_csv.return_value = mock_lookup_df
    
    mock_get_municipality_ids.return_value = {'Municipality1': 'ID1'}
    mock_get_taxon_data.return_value = pd.DataFrame({'id': [1], 'name': ['Species1']})
    mock_get_collection_names.return_value = {'Collection1': 'Name1'}
    mock_get_value_ranges.return_value = {'range1': 'value1'}
    mock_get_enumerations.return_value = {'enum1': 'label1'}
    
    config = {
        'laji_api_url': 'https://api.laji.fi/',
        'access_token': 'test_token'
    }
    
    # Clear cache to ensure fresh test
    cache_key = f"helper_data_{config['laji_api_url']}"
    load_data._cache.pop(cache_key, None)
    load_data._cache_timestamps.pop(cache_key, None)
    
    # Test first call - should fetch from API
    result = load_data.load_or_update_cache(config)
    
    # Verify all API calls were made
    mock_read_json.assert_called_once_with('scripts/resources/municipality_ely_mappings.json')
    mock_read_csv.assert_called_once_with('scripts/resources/lookup_table_columns.csv', sep=';', header=0)
    mock_get_municipality_ids.assert_called_once()
    mock_get_taxon_data.assert_called_once()
    mock_get_collection_names.assert_called_once()
    mock_get_value_ranges.assert_called_once()
    mock_get_enumerations.assert_called_once()
    
    # Verify result structure
    assert len(result) == 6
    municipality_ely_mappings, municipals_ids, lookup_df, taxon_df, collection_names, all_value_ranges = result
    assert municipals_ids == {'Municipality1': 'ID1'}
    assert collection_names == {'Collection1': 'Name1'}
    assert all_value_ranges == {'range1': 'value1', 'enum1': 'label1'}
    
    # Verify data is cached
    assert cache_key in load_data._cache
    assert cache_key in load_data._cache_timestamps
    
    # Reset mocks for second call
    mock_read_json.reset_mock()
    mock_read_csv.reset_mock()
    mock_get_municipality_ids.reset_mock()
    mock_get_taxon_data.reset_mock()
    mock_get_collection_names.reset_mock()
    mock_get_value_ranges.reset_mock()
    mock_get_enumerations.reset_mock()
    
    # Test second call - should use cache
    result2 = load_data.load_or_update_cache(config)
    
    # Verify no API calls were made
    mock_read_json.assert_not_called()
    mock_read_csv.assert_not_called()
    mock_get_municipality_ids.assert_not_called()
    mock_get_taxon_data.assert_not_called()
    mock_get_collection_names.assert_not_called()
    mock_get_value_ranges.assert_not_called()
    mock_get_enumerations.assert_not_called()
    
    # Verify same result returned
    assert result == result2
    
    # Clean up
    load_data._cache.pop(cache_key, None)
    load_data._cache_timestamps.pop(cache_key, None)

@patch('scripts.load_data.fetch_json_with_retry')
def test_get_filter_values(mock_fetch):
    # Test successful response with valid data
    mock_fetch.return_value = {
        'enumerations': [
            {'label': {'fi': 'Lintu'}, 'name': 'BIRD'},
            {'label': {'fi': 'Kasvi'}, 'name': 'PLANT'},
            {'label': {'fi': 'Sieni'}, 'name': 'FUNGI'},
            {'label': {}, 'name': 'NO_FINNISH_LABEL'},  # Should be filtered out
            {'name': 'NO_LABEL_FIELD'}  # Should be filtered out
        ]
    }
    
    result = load_data.get_filter_values('taxonGroup', 'test_token', 'https://api.laji.fi/')
    expected = {
        'Lintu': 'BIRD',
        'Kasvi': 'PLANT', 
        'Sieni': 'FUNGI'
    }
    assert result == expected
    mock_fetch.assert_called_once_with('https://api.laji.fi/warehouse/filters/taxonGroup', headers={'Authorization': 'Bearer test_token', 'Api-Version': '1'})
        
    # Test caching behavior - second call should return cached result
    mock_fetch.reset_mock()
    mock_fetch.return_value = {'enumerations': [{'label': {'fi': 'Cached'}, 'name': 'CACHED'}]}
    
    # First call
    result1 = load_data.get_filter_values('cached_filter', 'test_token', 'https://api.laji.fi/')
    assert mock_fetch.call_count == 1
    
    # Second call with same parameters should use cache
    result2 = load_data.get_filter_values('cached_filter', 'test_token', 'https://api.laji.fi/')
    assert mock_fetch.call_count == 1  # No additional API call
    assert result1 == result2

@patch('requests.get')
def test_fetch_json_with_retry(mock_get):
    # Test successful fetch
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"key": "value"}
    mock_get.return_value = mock_response
    result = load_data.fetch_json_with_retry("http://example.com/api")
    assert result == {"key": "value"}
    mock_get.assert_called_once_with("http://example.com/api", params=None, headers=None)

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

@patch('scripts.load_data.fetch_json_with_retry')
def test_get_collection_names(mock_fetch):
    mock_fetch.return_value = {
        'results': [
            {'id': '1', 'longName': 'Collection One'},
            {'id': '2', 'longName': 'Collection Two'}
        ]
    }
    params = {}
    headers = {}
    result = load_data.get_collection_names("http://example.com/api", params, headers)
    expected = {'1': 'Collection One', '2': 'Collection Two'}
    assert result == expected
    mock_fetch.assert_called_once_with("http://example.com/api", params=params, headers=headers)

def test_get_last_page():
    url = "https://beta.laji.fi/api/warehouse/query/unit/list"
    params = {'page': '1', 'pageSize': '1', 'geoJSON': 'true', 'featureType': 'ORIGINAL_FEATURE'}
    headers = {'Authorization': 'Bearer test_token', 'Api-Version': '1'}
    last_page = load_data.get_last_page(url, params, headers, page_size=1)
    assert isinstance(last_page, int)
    assert last_page >= 1

@patch('scripts.load_data.get_last_page')
def test_get_pages(mock_get_last_page):
    # all mode
    mock_get_last_page.return_value = 10
    params = {}
    headers = {}
    assert load_data.get_pages('all', 'http://example.com/url', params, headers, 10000) == 10
    mock_get_last_page.assert_called_once()

    # latest mode
    mock_get_last_page.reset_mock()
    mock_get_last_page.return_value = 5
    assert load_data.get_pages('latest', 'http://example.com/url', params, headers, 10000) == 5
    mock_get_last_page.assert_called_once()

    # numeric mode
    assert load_data.get_pages('7', 'http://example.com/url', params, headers, 10000) == 7

def test_download_page():
    url = "https://beta.laji.fi/api/warehouse/query/unit/list"
    params = {'page': '1', 'pageSize': '1', 'geoJSON': 'true', 'featureType': 'ORIGINAL_FEATURE'}
    headers = {'Authorization': 'Bearer test_token', 'Api-Version': '1'}
    gdf = load_data.download_page(url, params, headers, page_no=1)
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert not gdf.empty

def test_get_occurrence_data():
    url = "https://beta.laji.fi/api/warehouse/query/unit/list"
    params = {'page': '1', 'pageSize': '1', 'geoJSON': 'true', 'featureType': 'ORIGINAL_FEATURE', 'time': '/-1'}
    headers = {'Authorization': 'Bearer test_token', 'Api-Version': '1'}
    gdf, _ = load_data.get_occurrence_data(url=url, params=params, headers=headers, startpage=1, endpage=2, multiprocessing=True)
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert not gdf.empty
    assert gdf['geometry'].dtype == 'geometry'
    assert gdf['unit.unitId'].dtype == 'object'

    gdf2, _ = load_data.get_occurrence_data(url=url, params=params, headers=headers, startpage=1, endpage=2, multiprocessing=False)
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
    url = "https://beta.laji.fi/api/metadata/ranges"
    params = {'lang': 'fi', 'asLookupObject': 'true'}
    headers = {'Authorization': 'Bearer test_token', 'Api-Version': '1'}
    result = load_data.get_value_ranges(url, params, headers)
    assert isinstance(result, dict)
    assert 'MY.recordBasisIndirectSampleIndirectSample' in result

def test_get_taxon_data():
    taxon_name_url = f'https://beta.laji.fi/api/informal-taxon-groups'
    params = {'pageSize': '1'}
    headers = {'Authorization': 'Bearer test_token', 'Api-Version': '1'}
    df = load_data.get_taxon_data(taxon_name_url, params, headers)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert 'id' in df.columns
    assert 'name' in df.columns

def test_get_enumerations():
    url = "https://beta.laji.fi/api/warehouse/enumeration-labels"
    params = {}
    headers = {'Authorization': 'Bearer test_token', 'Api-Version': '1'}
    result = load_data.get_enumerations(url, params, headers)
    assert isinstance(result, dict)
    assert result['IMAGE'] == 'Kuva'

@patch('scripts.load_data.fetch_json_with_retry')
def test_get_municipality_ids(mock_fetch):
    # Test successful response with valid data
    mock_fetch.return_value = {
        'results': [
            {'name': 'Helsinki', 'id': 'ML.206'},
            {'name': 'Tampere', 'id': 'ML.837'},
            {'name': 'Turku', 'id': 'ML.853'}
        ]
    }
    
    params = {}
    headers = {}
    result = load_data.get_municipality_ids("http://example.com/api", params, headers)
    expected = {
        'Helsinki': 'ML.206',
        'Tampere': 'ML.837',
        'Turku': 'ML.853'
    }
    assert result == expected
    mock_fetch.assert_called_once_with("http://example.com/api", params=params, headers=headers)

    # Test when fetch_json_with_retry returns None
    mock_fetch.reset_mock()
    mock_fetch.return_value = None
    result = load_data.get_municipality_ids("http://example.com/api", params, headers)
    assert result is None
    mock_fetch.assert_called_once_with("http://example.com/api", params=params, headers=headers)
