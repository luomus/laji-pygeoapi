import pandas as pd
from scripts import convert_api_filters

# run with:
# cd pygeoapi
# python -m pytest tests/test_convert_api_filters.py -v

def test_convert_filters():
    lookup_df = pd.DataFrame({'selected': ['collectionId'], 'virva': ['Aineiston_tunniste'], 'finbif_api_query': ['collectionId']})
    all_value_ranges = {}
    municipals_ids = {'Helsinki': '123'}
    params = {}
    properties = [('Aineiston_tunniste', 'http://tun.fi/HR.95'), ('finnishMunicipalityId', 'Helsinki')]
    result = convert_api_filters.convert_filters(lookup_df, all_value_ranges, municipals_ids, params, properties, access_token='test_token')
    assert result['collectionId'] == 'HR.95'
    assert result['finnishMunicipalityId'] == '123'

def test_translate_filter_names():
    df = pd.DataFrame({'virva': ['Havainnon_tunniste'], 'finbif_api_query': ['unitId']})
    assert convert_api_filters.translate_filter_names(df, 'Havainnon_tunniste') == 'unitId'

def test_remove_tunfi_prefix():
    assert convert_api_filters.remove_tunfi_prefix('http://tun.fi/HR.95') == 'HR.95'
    assert convert_api_filters.remove_tunfi_prefix('http://id.luomus.fi/HR.95') == 'HR.95'
    assert convert_api_filters.remove_tunfi_prefix('http://id.luomus.fi/HR.95/ggg') == 'HR.95/ggg'
    assert convert_api_filters.remove_tunfi_prefix('HR.95') == 'HR.95'
    assert convert_api_filters.remove_tunfi_prefix(123) == 123

def test_map_value_ranges():
    all_value_ranges = {'A': 'x', 'B': 'y'}
    assert convert_api_filters.map_value_ranges(all_value_ranges, 'x') == 'A'
    assert convert_api_filters.map_value_ranges(all_value_ranges, 'y') == 'B'
    assert convert_api_filters.map_value_ranges(all_value_ranges, 'z') == 'z'
    assert convert_api_filters.map_value_ranges(all_value_ranges, 'x,y') == 'A,B'

def test_map_biogeographical_provinces():
    convert_api_filters.id_mapping = {'ML.251': 'Ahvenanmaa', 'ML.252': 'Varsinais-Suomi'}
    assert convert_api_filters.map_biogeographical_provinces('Ahvenanmaa') == 'ML.251'
    assert convert_api_filters.map_biogeographical_provinces('Varsinais-Suomi') == 'ML.252'
    assert convert_api_filters.map_biogeographical_provinces('vaRsinais-suomi') == 'ML.252'
    assert convert_api_filters.map_biogeographical_provinces('Ahvenanmaa,Varsinais-Suomi') == 'ML.251,ML.252'

def test_map_value():
    # Mock the get_filter_values function
    original_get_filter_values = convert_api_filters.get_filter_values
    
    def mock_get_filter_values(filter_name, access_token):
        if filter_name == 'sex':
            return {'naaras': 'FEMALE', 'koiras': 'MALE'}
        return {}
    
    convert_api_filters.get_filter_values = mock_get_filter_values
    
    try:
        assert convert_api_filters.map_value('naaras', 'sex', 'access_token') == 'FEMALE'
        assert convert_api_filters.map_value('koiras,naaras', 'sex', 'access_token') == 'MALE,FEMALE'
        assert convert_api_filters.map_value('unknown', 'sex', 'access_token') == 'unknown'
    finally:
        # Restore original function
        convert_api_filters.get_filter_values = original_get_filter_values

def test_map_municipality():
    municipals_ids = {'Helsinki': '1', 'Enontekiö': '2'}
    assert convert_api_filters.map_municipality(municipals_ids, 'Helsinki') == '1'
    assert convert_api_filters.map_municipality(municipals_ids, 'Enontekiö') == '2'
    assert convert_api_filters.map_municipality(municipals_ids, 'Kunta ruotsista?') == 'Kunta ruotsista?'

def test_convert_time():
    assert convert_api_filters.convert_time('2020.01.01 [9:41]') == '2020.01.01'
    assert convert_api_filters.convert_time('2020, 2021') == '2020,2021'
    assert convert_api_filters.convert_time('-7 / 0 ') == '-7/0'
    assert convert_api_filters.convert_time('2020/2021') == '2020/2021'
    assert convert_api_filters.convert_time(123) == 123
    assert convert_api_filters.convert_time('2020-01-01 [9:41] / 2025-12-31 [9:43]') == '2020-01-01/2025-12-31'

def test_process_bbox():
    # Test WGS84 coordinates (EPSG:4326)

    bbox_tm35fin = [376244.4479,6664797.5738,401678.9648,6678720.0844]  # xmin, ymin, xmax, ymax
    bbox_wgs84 = [24.7741,60.1014,25.2246,60.2333] # xmin, ymin, xmax, ymax

    result_tm35fin = convert_api_filters.process_bbox(bbox_tm35fin)
    result_wgs84 = convert_api_filters.process_bbox(bbox_wgs84)

    # Basic structure check
    assert result_wgs84.startswith('POLYGON((') and result_tm35fin.startswith('POLYGON((')
    assert 'POLYGON((6664797.5738 376244.4479, 6678720.0844 376244.4479, 6678720.0844 401678.9648, 6664797.5738 401678.9648, 6664797.5738 376244.4479))' in result_tm35fin
    assert 'POLYGON((3969942.835222635 3194502.392363714, 3969395.3907633997 3194502.392363714, 3969395.3907633997 3253996.7532844027, 3969942.835222635 3253996.7532844027, 3969942.835222635 3194502.392363714))' in result_wgs84
