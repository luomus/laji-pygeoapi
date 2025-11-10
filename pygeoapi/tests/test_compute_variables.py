import pandas as pd
import numpy as np
from shapely.geometry import Point, LineString
import geopandas as gpd

from scripts import compute_variables

# run with:
# cd pygeoapi
# python -m pytest tests/test_compute_variables.py -v

def test_compute_individual_count():
    col = pd.Series([0, 1, 10, np.nan, None, -5])
    result = compute_variables.compute_individual_count(col)
    assert list(result) == ['poissa', 'paikalla', 'paikalla', None, None, 'poissa']

def test_compute_collection_id():
    collection_names = {'HR.3553': 'Collection A', 'HR.1747': 'Lajitietokeskus/FinBIF - Vihkon yleiset havainnot'}
    col = pd.Series(['http://tun.fi/HR.3553', 'http://tun.fi/HR.1747', 'HR.0000'])
    result = compute_variables.compute_collection_id(col, collection_names)
    assert result[0] == 'Collection A'
    assert result[1] == 'Lajitietokeskus/FinBIF - Vihkon yleiset havainnot'
    assert pd.isna(result[2])

def test_map_values():
    value_ranges = {
        'MX.regionallyThreatened2020_4d': 'Alueellisesti uhanalainen',
        'MX.gameBird': 'Metsästyslaissa luetellut riistalinnut',
        'MX.gameMammal': 'Metsästyslaissa luetellut riistanisäkkäät (5§)'
    }
    col = pd.Series(['MX.regionallyThreatened2020_4d'])
    result = compute_variables.map_values(col, value_ranges)
    assert result[0] == 'Alueellisesti uhanalainen'

    col2 = pd.Series(['http://tun.fi/MX.gameBird, MX.gameMammal, xyz123'])
    result2 = compute_variables.map_values(col2, value_ranges)
    assert result2[0] == 'Metsästyslaissa luetellut riistalinnut, Metsästyslaissa luetellut riistanisäkkäät (5§), xyz123'

    col3 = pd.Series(['http://tun.fi/MX.gameBird, http://luomus.fi/MX.gameMammal, http://asd.luomus.asd.fi/xyz123, 123123'])
    result3 = compute_variables.map_values(col3, value_ranges)
    assert result3[0] == 'Metsästyslaissa luetellut riistalinnut, Metsästyslaissa luetellut riistanisäkkäät (5§), http://asd.luomus.asd.fi/xyz123, 123123'

def test_compute_areas():
    municipality_ely_mappings = pd.read_json('scripts/resources/municipality_ely_mappings.json').set_index('Municipal_Name')['ELY_Area_Name']

    gdf_with_geom_and_ids = gpd.GeoDataFrame({
        'unit.unitId': ['1', '2', '3'],
        'gathering.interpretations.municipalityDisplayname': ['Helsinki', 'Espoo', 'Kuhmo, Nurmes'],
        'geometry': [Point(24.941, 60.169), Point(24.655, 60.205), LineString([(24.655, 60.205), (24.941, 60.169)])]
    }, crs="EPSG:4326")

    result_ely_area = compute_variables.compute_areas(gdf_with_geom_and_ids['gathering.interpretations.municipalityDisplayname'], municipality_ely_mappings)

    assert result_ely_area[0] == 'Uudenmaan ELY-keskus'
    assert result_ely_area[1] == 'Uudenmaan ELY-keskus'
    assert result_ely_area[2] == 'Kainuun ELY-keskus, Pohjois-Karjalan ELY-keskus'


def test_get_title_name_from_table_name():
    assert compute_variables.get_title_name_from_table_name("sompion_lappi_polygons") == "Sompion Lappi"
    assert compute_variables.get_title_name_from_table_name("kittilan_lappi_lines") == "Kittilän Lappi"
    assert compute_variables.get_title_name_from_table_name("unknown_area_polygons") == "Finland"


def test_get_biogeographical_region_from_id():
    assert compute_variables.get_biogeographical_region_from_id("ML.251") == "ahvenanmaa"
    assert compute_variables.get_biogeographical_region_from_id("ML.270") == "enontekion_lappi"
    assert compute_variables.get_biogeographical_region_from_id(None) == "empty_biogeographical_region"

def test_process_strip_url_columns():
    gdf = pd.DataFrame({
        'unit.atlasClass': ['http://tun.fi/atlasA'],
        'unit.atlasCode': ['code1']
    })
    value_ranges = {'atlasA': 'Atlas A', 'code1': 'Code 1'}
    result = compute_variables.process_strip_url_columns(gdf, value_ranges)
    assert result['unit.atlasClass'][0] == 'Atlas A'
    assert result['unit.atlasCode'][0] == 'Code 1'

def test_process_direct_map_columns():
    gdf = pd.DataFrame({'unit.recordBasis': ['PRESERVED_SPECIMEN']})
    value_ranges = {'PRESERVED_SPECIMEN': 'Näyte'}
    result = compute_variables.process_direct_map_columns(gdf, value_ranges)
    assert result['unit.recordBasis'][0] == 'Näyte'

def test_compute_all(tmp_path):
    # Minimal test for compute_all
    gdf = gpd.GeoDataFrame({
        'unit.atlasClass': ['http://tun.fi/atlasA'],
        'unit.atlasCode': ['http://tun.fi/code1'],
        'unit.linkings.taxon.primaryHabitat.habitat': ['http://tun.fi/habitatMkt'],
        'unit.linkings.taxon.latestRedListStatusFinland.status': ['http://tun.fi/iucnLC'],
        'unit.linkings.taxon.threatenedStatus': ['threatened'],
        'unit.recordBasis': ['PRESERVED_SPECIMEN'],
        'unit.interpretations.recordQuality': ['EXPERT_VERIFIED'],
        'document.secureReasons': ['DEFAULT_TAXON_CONSERVATION'],
        'unit.sex': ['MALE'],
        'unit.abundanceUnit': ['INDIVIDUAL_COUNT'],
        'document.linkings.collectionQuality': ['PROFESSIONAL'],
        'unit.linkings.taxon.administrativeStatuses': ['http://tun.fi/MX.birdsDirectiveStatusAppendix2A, http://tun.fi/MX.birdsDirectiveStatusAppendix3A'],
        'unit.interpretations.individualCount': [3],
        'document.collectionId': ['HR.1747'],
        'unit.unitId': ['1'],
        'gathering.interpretations.municipalityDisplayname': ['Helsinki'],
        'geometry': [Point(24.941, 60.169)]
    }, crs="EPSG:4326")
    value_ranges = {
        'atlasA': 'Atlas A',
        'code1': 'Code 1',
        'habitatMkt': 'Mkt – tuoreet ja lehtomaiset kankaat',
        'iucnLC': 'LC – Elinvoimaiset',
        'threatened': 'Threatened',
        'PRESERVED_SPECIMEN': 'Näyte',
        'EXPERT_VERIFIED': 'Asiantuntijan varmistama',
        'DEFAULT_TAXON_CONSERVATION': 'Lajitiedon sensitiivisyys',
        'MALE': 'koiras',
        'INDIVIDUAL_COUNT': 'Yksilömäärä',
        'PROFESSIONAL': 'Ammattiaineistot / asiantuntijoiden laadunvarmistama',
        'MX.birdsDirectiveStatusAppendix2A': 'EU:n lintudirektiivin II/A-liite',
        'MX.birdsDirectiveStatusAppendix3A': 'EU:n lintudirektiivin III/A-liite'
    }
    collection_names = {'HR.1747': 'Lajitietokeskus/FinBIF - Vihkon yleiset havainnot'}

    municipality_ely_mappings = pd.read_json('scripts/resources/municipality_ely_mappings.json').set_index('Municipal_Name')['ELY_Area_Name']

    result_gdf = compute_variables.compute_all(gdf, value_ranges, collection_names, municipality_ely_mappings)
    assert result_gdf['unit.atlasClass'][0] == 'Atlas A'
    assert result_gdf['unit.atlasCode'][0] == 'Code 1'
    assert result_gdf['unit.linkings.taxon.primaryHabitat.habitat'][0] == 'Mkt – tuoreet ja lehtomaiset kankaat'
    assert result_gdf['unit.linkings.taxon.latestRedListStatusFinland.status'][0] == 'LC – Elinvoimaiset'
    assert result_gdf['unit.linkings.taxon.threatenedStatus'][0] == 'Threatened'
    assert result_gdf['unit.recordBasis'][0] == 'Näyte'
    assert result_gdf['unit.interpretations.recordQuality'][0] == 'Asiantuntijan varmistama'
    assert result_gdf['document.secureReasons'][0] == 'Lajitiedon sensitiivisyys'
    assert result_gdf['unit.sex'][0] == 'koiras'
    assert result_gdf['unit.abundanceUnit'][0] == 'Yksilömäärä'
    assert result_gdf['document.linkings.collectionQuality'][0] == 'Ammattiaineistot / asiantuntijoiden laadunvarmistama'
    assert result_gdf['unit.linkings.taxon.administrativeStatuses'][0] == 'EU:n lintudirektiivin II/A-liite, EU:n lintudirektiivin III/A-liite'
    assert result_gdf['Esiintyman_tila'][0] == 'paikalla'
    assert result_gdf['Aineisto'][0] == 'Lajitietokeskus/FinBIF - Vihkon yleiset havainnot'
    assert result_gdf['Vastuualue'][0] == 'Uudenmaan ELY-keskus'
    assert result_gdf['Paikallinen_tunniste'][0] == '1'