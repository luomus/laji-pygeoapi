import os, sys
import tempfile

import pandas as pd
from pygeoapi.scripts import convert_api_filters

# run with:
# python -m pytest tests/test_convert_api_filters.py -v

def test_convert_filters():
    lookup_df = pd.DataFrame({'virva': ['Aineiston_tunniste'], 'finbif_api_query': ['collectionId']})
    all_value_ranges = {'HUMAN_OBSERVATION_UNSPECIFIED': 'Havaittu'}
    municipals_ids = {'Helsinki': '123'}
    params = {}
    properties = [('Aineiston_tunniste', 'http://tun.fi/HR.95'), ('recordBasis', 'Havaittu'), ('finnishMunicipalityId', 'Helsinki')]
    result = convert_api_filters.convert_filters(lookup_df, all_value_ranges, municipals_ids, params, properties)
    assert result['collectionId'] == 'HR.95'
    assert result['recordBasis'] == 'HUMAN_OBSERVATION_UNSPECIFIED'
    assert result['finnishMunicipalityId'] == '123'

def test_translate_filter_names():
    df = pd.DataFrame({'virva': ['foo'], 'finbif_api_query': ['bar']})
    assert convert_api_filters.translate_filter_names(df, 'foo') == 'bar'
    assert convert_api_filters.translate_filter_names(df, 'baz') == 'baz'

def test_remove_tunfi_prefix():
    assert convert_api_filters.remove_tunfi_prefix('http://tun.fi/HR.95') == 'HR.95'
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

def test_map_sex():
    assert convert_api_filters.map_sex('naaras') == 'FEMALE'
    assert convert_api_filters.map_sex('koiras,naaras') == 'MALE,FEMALE'
    assert convert_api_filters.map_sex('unknown') == 'unknown'

def test_map_lifestage():
    assert convert_api_filters.map_lifestage('aikuinen') == 'ADULT'
    assert convert_api_filters.map_lifestage('nuori,aikuinen') == 'JUVENILE,ADULT'
    assert convert_api_filters.map_lifestage('foobar') == 'foobar'

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
    # Test bbox conversion from WGS84 to EUREF-TM35FIN WKT polygon
    bbox = [24.0, 60.0, 25.0, 61.0]  # minx, miny, maxx, maxy in WGS84
    result = convert_api_filters.process_bbox(bbox)
    
    # Check that result is a WKT POLYGON string
    assert result.startswith('POLYGON((')
    assert '332705.1788734832 6655205.483511592' in result

    # Check that polygon is closed (first and last coordinate pairs are the same)
    coords_part = result[9:-2]  # Remove 'POLYGON((' and '))'
    coord_pairs = coords_part.split(', ')
    assert coord_pairs[0] == coord_pairs[-1]
    
    # Check that we have 5 coordinate pairs (4 corners + closing)
    assert len(coord_pairs) == 5
