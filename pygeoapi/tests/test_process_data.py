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

def test_merge_duplicates():
    from shapely.geometry import Point
    data = {
        'Keruu_aloitus_pvm': ['2020-01-01', '2020-01-01', '2020-01-01', '2020-02-01'],
        'Keruu_lopetus_pvm': ['2020-01-02', '2020-01-02', '2020-01-02', '2020-02-02'],
        'ETRS_TM35FIN_WKT': ['POINT (1 1)', 'POINT (1 1)', 'POINT (1 1)', 'POINT (2 2)'],
        'Tieteellinen_nimi': ['asd', 'asd', 'asd', 'juu'],
        'Paikan_tarkkuus_metreina': ['asd', 'asd', 'asd', 'juu'],
        'Eliomaakunta': ['asd', 'asd', 'asd', 'juu'],
        'Aineiston_tunniste': ['asd', 'asd', 'asd', 'juu'],
        'Pesintapaikka': [True, True, True, False],
        'ETRS_TM35FIN_WKT': ['asd', 'asd', 'asd', 'juu'],
        'Aika': ['asd', 'asd', 'asd', 'juu'],
        'Sijainti': ['asd', 'asd', 'asd', 'juu'],
        'Kunta': ['asd', 'asd', 'asd', 'juu'],
        'Aineisto': ['asd', 'asd', 'asd', 'juu'],
        'Avainsanat': ['asd', 'asd', 'asd', 'juu'],
        'Havainnon_lisatiedot': ['asd', 'asd', 'asd', 'juu'],
        'Maara': ['asd', 'asd', 'asd', 'juu'],
        'Suomenkielinen_nimi': ['asd', 'asd', 'asd', 'juu'],
        'Ruotsinkielinen_nimi': ['asd', 'asd', 'asd', 'juu'],
        'Taksonominen_jarjestys': ['asd', 'asd', 'asd', 'juu'],
        'Aineiston_laatu': ['asd', 'asd', 'asd', 'juu'],
        'Uhanalaisuusluokka': ['asd', 'asd', 'asd', 'juu'],
        'Hallinnollinen_asema': ['asd', 'asd', 'asd', 'juu'],
        'Sensitiivinen_laji': [True, True, True, False],
        'ETRS_TM35FIN_N': ['asd', 'asd', 'asd', 'juu'],
        'ETRS_TM35FIN_E': ['asd', 'asd', 'asd', 'juu'],
        'Ensisijainen_biotooppi': ['asd', 'asd', 'asd', 'juu'],
        'Atlasluokka': ['asd', 'asd', 'asd', 'juu'],
        'Aineistolahde': ['asd', 'asd', 'asd', 'juu'],
        'Seurantapaikan_tila': ['asd', 'asd', 'asd', 'juu'],
        'Seurantapaikan_tyyppi': ['asd', 'asd', 'asd', 'juu'],
        'Valtion_maalla': [True, True, True, False],
        'Seurattava_laji': ['asd', 'asd', 'asd', 'juu'],
        'Maaran_yksikko': ['asd', 'asd', 'asd', 'juu'],
        'Sijainnin_tarkkuusluokka': ['asd', 'asd', 'asd', 'juu'],
        'Havainnon_laatu': ['asd', 'asd', 'asd', 'juu'],
        'Peittavyysprosentti': ['asd', 'asd', 'asd', 'juu'],
        'Havainnon_maaran_yksikko': ['asd', 'asd', 'asd', 'juu'],
        'Havainnoijat': ['A K', 'A K', 'A K', 'B D'],
        'Taksonin_tunniste': ['T1', 'T1', 'T1', 'T2'],
        'Yksilomaara_tulkittu': [5, 2, 20, 10],
        'Keruutapahtuman_tunniste': ['K1', 'K2', 'K4', 'K3'],
        'Havainnon_tunniste': ['H1', 'H2', 'H4', 'H3'],
        'geometry': [Point(1, 1), Point(1, 1), Point(1, 1), Point(2, 2)]
    }
    gdf = gpd.GeoDataFrame(data, crs="EPSG:4326")
    lookup_df = pd.read_csv('scripts/resources/lookup_table_columns.csv', sep=';', header=0)

    merged_gdf, _ = process_data.merge_duplicates(gdf, lookup_df)
    assert len(merged_gdf) == 2
    assert merged_gdf.loc[0, 'Yksilomaara_tulkittu'] == 27
    assert merged_gdf.loc[0, 'Keruutapahtuman_tunniste'] == 'K1, K2, K4'
    assert merged_gdf.loc[0, 'Havainnon_tunniste'] == 'H1, H2, H4'
    assert merged_gdf.loc[1, 'Keruutapahtuman_tunniste'] == 'K3'
    assert merged_gdf.loc[1, 'Havainnon_tunniste'] == 'H3'

def test_process_facts():
    gdf = gpd.GeoDataFrame({
        'gathering.facts[0].fact': ['Seurattava laji', 'Sijainnin tarkkuusluokka', 'Peittävyysprosentti'],
        'gathering.facts[0].value': ['Laji1', 'Tarkkuus1', '10%'],
        'gathering.facts[1].fact': ['Havainnon laatu', 'Havainnon määrän yksikkö', 'randon column'],
        'gathering.facts[1].value': ['Laatu1', 'Yksikkö1', None],
        'geometry': [None, None, None]
    })
    result_gdf = process_data.process_facts(gdf)
    assert 'gathering.facts[0].fact' not in result_gdf.columns
    assert 'gathering.facts[0].value' not in result_gdf.columns
    assert 'randon column' not in result_gdf.columns
    assert result_gdf['Seurattava laji'].iloc[0] == 'Laji1'
    assert result_gdf['Sijainnin tarkkuusluokka'].iloc[1] == 'Tarkkuus1'
    assert result_gdf['Peittävyysprosentti'].iloc[2] == '10%'
    assert result_gdf['Havainnon laatu'].iloc[0] == 'Laatu1'
    assert result_gdf['Havainnon määrän yksikkö'].iloc[1] == 'Yksikkö1'