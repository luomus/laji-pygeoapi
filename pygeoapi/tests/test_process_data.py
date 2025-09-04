import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon, LineString, GeometryCollection, MultiPolygon
from pandas.testing import assert_frame_equal

from scripts import process_data

# run with:
# cd pygeoapi
# python -m pytest tests/test_process_data.py -v

def test_merge_taxonomy_data():
    data_occurrence = {
        'unit.linkings.originalTaxon.informalTaxonGroups[0]': [
            "http://tun.fi/MVL.26280", "http://tun.fi/MVL.27899", "http://tun.fi/MVL.27801", "http://tun.fi/MVL.27800"
        ],
        'some_other_column': [1, 2, 3, 4],
        'geometry': [Point(1, 1), Point(2, 2), Point(3, 3), Point(4, 4)]
    }
    occurrence_gdf = gpd.GeoDataFrame(data_occurrence, geometry='geometry')
    data_taxonomy = {
        'id': ["MVL.26280", 'MVL.27801', 'MVL.27800'],
        'taxon_name': ['Taxon A', 'Taxon B', 'Taxon C']
    }
    taxonomy_df = pd.DataFrame(data_taxonomy)
    merged_gdf = process_data.merge_taxonomy_data(occurrence_gdf, taxonomy_df)
    assert merged_gdf.shape[1] == occurrence_gdf.shape[1] + 2
    assert merged_gdf.shape[0] == occurrence_gdf.shape[0]
    assert 'taxon_name' in merged_gdf.columns
    assert merged_gdf.loc[0, 'taxon_name'] == 'Taxon A'
    assert merged_gdf.loc[2, 'taxon_name'] == 'Taxon B'
    assert merged_gdf.loc[3, 'taxon_name'] == 'Taxon C'
    assert pd.isna(merged_gdf.loc[1, 'taxon_name'])

def test_validate_geometry():
    valid_line = LineString([(0, 0), (1, 1)])
    valid_point = Point(2, 2)
    invalid_polygon = Polygon([(0, 0), (2, 0), (0, 2), (2, 2), (0, 0)])
    invalid_polygon2 = MultiPolygon([Polygon([(0, 0), (2, 0), (2, 2), (0, 2), (0, 0)]), Polygon([(1, 1), (3, 1), (3, 3), (1, 3), (1, 1)])])
    invalid_polygon3 = Polygon([(0, 0), (4, 0), (4, 4), (0, 4), (0, 0)], holes=[[(2, 2), (6, 2), (6, 6), (2, 6), (2, 2)]])
    gdf = gpd.GeoDataFrame({'geometry': [valid_line, valid_point, invalid_polygon, invalid_polygon2, invalid_polygon3]})
    result_gdf, edited_count = process_data.validate_geometry(gdf)
    assert edited_count == 3
    assert result_gdf.is_valid.all()

def test_combine_similar_columns():
    gdf = gpd.GeoDataFrame({
        'keyword[0]': ['a', None, 1],
        'keyword[1]': [None, 1.2345, 'd'],
        'other[0]': ['1', None, '3'],
        'other[1]': ['2', '2', 'asd'],
        'geometry': [None, None, None]
    })
    expected_gdf = gdf.drop(columns=['keyword[0]', 'keyword[1]', 'other[0]', 'other[1]']).copy()
    expected_gdf['keyword'] = ['a', '1.2345', '1, d']
    expected_gdf['other'] = ['1, 2', '2', '3, asd']
    result_gdf = process_data.combine_similar_columns(gdf.copy())
    assert_frame_equal(result_gdf, expected_gdf)

def test_translate_column_names():
    lookup_df = pd.read_csv('scripts/resources/lookup_table_columns.csv', sep=';', header=0)
    gdf = gpd.GeoDataFrame({
        'unit.unitId': [1, 2, 3],
        'unit.linkings.taxon.scientificName': ['asd', 'asd1', 'asd2'],
        'unit.interpretations.individualCount': [0, 1, 2],
        'extra_column': ['x', 'y', 'z']
    })
    result_gdf = process_data.translate_column_names(gdf, lookup_df, style='virva')
    assert 'extra_column' not in result_gdf.columns
    assert 'Havainnon_tunniste' in result_gdf.columns
    assert 'Sukupuoli' in result_gdf.columns
    assert result_gdf['Yksilomaara_tulkittu'].dtype == pd.Int64Dtype()
    assert len(result_gdf.columns) > 50

def test_convert_geometry_collection_to_multipolygon():
    point = Point(1, 1)
    line = LineString([(0, 0), (1, 1)])
    polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    gdf = gpd.GeoDataFrame({'geometry': [
        GeometryCollection([point, line]),
        GeometryCollection([polygon]),
        GeometryCollection([point, polygon]),
        None,
        polygon
    ]})
    gdf_converted, count = process_data.convert_geometry_collection_to_multipolygon(gdf, buffer_distance=0.5)
    assert isinstance(gdf_converted.loc[0, 'geometry'], MultiPolygon)
    assert isinstance(gdf_converted.loc[1, 'geometry'], Polygon)
    assert isinstance(gdf_converted.loc[2, 'geometry'], MultiPolygon)
    assert gdf_converted.loc[3, 'geometry'] is None
    assert isinstance(gdf_converted.loc[4, 'geometry'], Polygon)
    assert count == 3