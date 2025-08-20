from unittest.mock import Mock, patch
import pandas as pd
import sys

# Create a mock BaseProvider class that we can inherit from
class MockBaseProvider:
    def __init__(self, provider_def):
        pass

# Mock the pygeoapi.provider.base module before importing
sys.modules['pygeoapi.provider.base'] = Mock()
sys.modules['pygeoapi.provider.base'].BaseProvider = MockBaseProvider
sys.modules['pygeoapi.provider.base'].ProviderQueryError = Exception

# Now import the provider
from plugins import lajiapi_provider
LajiApiProvider = lajiapi_provider.LajiApiProvider

# run with:
# cd pygeoapi
# python -m pytest tests/test_lajiapi_provider.py -v

# Common test data and mocks
MOCK_CONFIG = {
    'laji_api_url': 'https://api.laji.fi/',
    'access_token': 'test_token'
}

MOCK_LOOKUP_DF = pd.DataFrame([
    {'finbif_api_query': 'test_query', 'virva': 'test_field', 'type': 'str'}
])


def create_test_provider():
    """Helper function to create a test provider with common mocks"""
    with patch('plugins.lajiapi_provider.setup_environment', return_value=MOCK_CONFIG), \
         patch('plugins.lajiapi_provider.load_or_update_cache', 
               return_value=(None, None, MOCK_LOOKUP_DF, None, None, None)):
        provider_def = {'name': 'test_provider'}
        return LajiApiProvider(provider_def)


def test_init():
    """Test provider initialization"""
    provider = create_test_provider()
    assert provider is not None


def test_get_fields():
    """Test get_fields method returns correct field types"""
    provider = create_test_provider()
    result = provider.get_fields()
    assert isinstance(result, dict)
    assert 'test_field' in result
    assert result['test_field']['type'] == 'string'


def test_fields_property():
    """Test fields property returns same as get_fields method"""
    provider = create_test_provider()
    fields_method = provider.get_fields()
    fields_property = provider.fields
    assert fields_method == fields_property

def test_build_request_params():
    """Test _build_request_params method with different parameters"""
    # Mock convert_filters to just return the params unchanged
    def mock_convert_filters(lookup_df, all_value_ranges, municipals_ids, params, properties):
        return params
    
    # Mock process_bbox to return a simple string
    def mock_process_bbox(bbox):
        return 'test_polygon'
    
    with patch('plugins.lajiapi_provider.convert_filters', side_effect=mock_convert_filters), \
         patch('plugins.lajiapi_provider.process_bbox', side_effect=mock_process_bbox):
        
        provider = create_test_provider()
        bbox = [24.934, 60.169, 24.941, 60.173]
        
        # Test basic parameters
        result = provider._build_request_params(offset=0, limit=100, bbox=bbox, properties=[])
        assert isinstance(result, dict)
        assert result['page'] == 1
        assert result['pageSize'] == 100
        assert result['polygon'] == 'test_polygon'
        assert 'selected' in result


def test_make_api_request():
    """Test _make_api_request method with successful and error responses"""
    # Test successful response
    mock_response = Mock()
    mock_response.json.return_value = {
        'total': 10,
        'pageSize': 5,
        'results': []
    }
    mock_response.raise_for_status.return_value = None
    
    with patch('plugins.lajiapi_provider.requests.get', return_value=mock_response):
        provider = create_test_provider()
        
        params = {
            'page': 1,
            'pageSize': 100,
            'access_token': 'test_token'
        }
        
        result = provider._make_api_request(params)
        assert isinstance(result, dict)
        assert result['total'] == 10
        assert result['pageSize'] == 5

    # Test case when there are too many items (> 1 million) and page > 1
    mock_response_too_many = Mock()
    mock_response_too_many.json.return_value = {
        'total': 1_500_000,  # More than 1 million
        'pageSize': 1000,
        'results': []
    }
    mock_response_too_many.raise_for_status.return_value = None
    
    with patch('plugins.lajiapi_provider.requests.get', return_value=mock_response_too_many):
        provider = create_test_provider()
        
        params_too_many = {
            'page': 2,  # Page > 1
            'pageSize': 1000,
            'access_token': 'test_token'
        }
        
        # Should raise ProviderQueryError
        try:
            provider._make_api_request(params_too_many)
            assert False, "Expected ProviderQueryError to be raised"
        except Exception as e:
            assert "Too many items in response" in str(e)

def test_query():
    """Test query method with different result types"""
    # Mock the API response
    mock_response = Mock()
    mock_response.json.return_value = {
        'total': 5,
        'pageSize': 5,
        'results': []
    }
    mock_response.raise_for_status.return_value = None
    
    # Mock process_json_features to return simple features
    mock_features = [{'type': 'Feature', 'properties': {}, 'geometry': {}}]
    
    # Mock the dependencies
    def mock_convert_filters(lookup_df, all_value_ranges, municipals_ids, params, properties):
        return params
    
    def mock_process_bbox(bbox):
        return 'test_polygon'
    
    with patch('plugins.lajiapi_provider.requests.get', return_value=mock_response), \
         patch('plugins.lajiapi_provider.convert_filters', side_effect=mock_convert_filters), \
         patch('plugins.lajiapi_provider.process_bbox', side_effect=mock_process_bbox), \
         patch('plugins.lajiapi_provider.process_json_features', return_value=mock_features):
        
        provider = create_test_provider()
        
        # Test basic query
        result = provider.query(offset=0, limit=100)
        assert isinstance(result, dict)
        assert 'features' in result
        assert 'numberReturned' in result
        assert result['numberReturned'] == 1  # Length of mock_features
        assert result['features'] == mock_features
        
        # Test hits query (should return empty features)
        hits_result = provider.query(resulttype='hits')
        assert hits_result['type'] == 'FeatureCollection'
        assert hits_result['features'] == []
        assert hits_result['numberMatched'] == 5  # From mock response total


def test_get():
    """Test get method for retrieving single records"""
    # Mock the API response
    mock_response = Mock()
    mock_response.json.return_value = {
        'total': 1,
        'pageSize': 1,
        'results': []
    }
    mock_response.raise_for_status.return_value = None
    
    # Mock process_json_features to return a single feature
    mock_feature = {
        'type': 'Feature',
        'properties': {'unitId': 'http://tun.fi/12345'},
        'geometry': {'type': 'Point', 'coordinates': [24.0, 60.0]}
    }
    mock_features = [mock_feature]
    
    with patch('plugins.lajiapi_provider.requests.get', return_value=mock_response), \
         patch('plugins.lajiapi_provider.process_json_features', return_value=mock_features):
        
        provider = create_test_provider()
        
        # Test get with identifier (should convert _ to #)
        identifier = '12345'
        result = provider.get(identifier)
        
        # Basic assertions
        assert isinstance(result, dict)
        assert result == mock_feature
        assert mock_response.json.called


def test_get_schema():
    """Test get_schema method returns correct JSON schema"""
    provider = create_test_provider()
    
    # Call get_schema
    content_type, schema = provider.get_schema()
    
    # Basic assertions
    assert content_type == "application/schema+json"
    assert isinstance(schema, dict)
    
    # Check schema structure
    assert schema['$schema'] == "http://json-schema.org/draft/2020-12/schema#"
    assert schema['title'] == "Feature properties"
    assert schema['type'] == "object"
    assert 'properties' in schema
    
    # Check that properties contains our test field
    properties = schema['properties']
    assert isinstance(properties, dict)
    assert 'test_field' in properties  # From our mock lookup_df
    assert properties['test_field']['type'] == 'string'  # str -> string mapping


def test_repr():
    """Test __repr__ method returns correct string representation"""
    provider = create_test_provider()
    repr_str = repr(provider)
    assert isinstance(repr_str, str)
    assert '<LajiApiProvider>' in repr_str
    assert 'https://api.laji.fi/' in repr_str
