import unittest
import os, sys
import pandas as pd
import numpy as np
from shapely.geometry import Point, Polygon
import geopandas as gpd
import pandas as pd
import unittest
from dotenv import load_dotenv


sys.path.append('src/')

from compute_variables import compute_all, get_biogeographical_region_from_id, get_title_name_from_table_name, map_values, compute_collection_id, compute_individual_count
from load_data import get_value_ranges, get_enumerations

class TestComputeAll(unittest.TestCase):

    def setUp(self):
        # Sample data for testing
        self.gdf = gpd.GeoDataFrame({
            'unit.atlasClass': ['http://tun.fi/MY.atlasClassEnumA', 'http://tun.fi/MY.atlasClassEnumB'],
            'unit.atlasCode': ['http://tun.fi/MY.atlasCodeEnum1', 'http://tun.fi/MY.atlasCodeEnum2'],
            'unit.linkings.originalTaxon.primaryHabitat.habitat': ['http://tun.fi/MKV.habitatM', 'http://tun.fi/MKV.habitatMk'],
            'unit.linkings.originalTaxon.latestRedListStatusFinland.status': ['http://tun.fi/MX.iucnEX', 'http://tun.fi/MX.iucnEW'],
            'unit.linkings.taxon.threatenedStatus': ['http://tun.fi/MX.threatenedStatusStatutoryProtected', 'http://tun.fi/MX.threatenedStatusThreatened'],
            'unit.recordBasis': ['PRESERVED_SPECIMEN', 'LIVING_SPECIMEN'],
            'unit.interpretations.recordQuality': ['EXPERT_VERIFIED', 'COMMUNITY_VERIFIED'],
            'document.secureReasons': ['DEFAULT_TAXON_CONSERVATION', 'NATURA_AREA_CONSERVATION'],
            'unit.sex': ['MALE', 'FEMALE'],
            'unit.abundanceUnit': ['INDIVIDUAL_COUNT', 'PAIR_COUNT'],
            'document.linkings.collectionQuality': ['PROFESSIONAL', 'HOBBYIST'],
            'unit.linkings.originalTaxon.administrativeStatuses': ['http://tun.fi/MX.finlex160_1997_appendix4_2021, http://tun.fi/MX.finlex160_1997_appendix4_specialInterest_2021', 'http://tun.fi/MX.finlex160_1997_appendix2a'],
            'unit.interpretations.individualCount': [3, 0],
            'document.collectionId': ['http://tun.fi/HR.1234', 'http://tun.fi/HR.5678'],
            'unit.unitId': ["http://tun.fi/1", "http://tun.fi/2"],
            'geometry': [Point(24.941, 60.169), Point(24.941, 61.0)]
        }, crs="EPSG:4326")

        self.collection_names = {
            'HR.1234': 'Collection 1',
            'HR.5678': 'Collection 2'
        }

    def test_compute_all(self):     
        load_dotenv()
        access_token = os.getenv('ACCESS_TOKEN')
        laji_api_url = os.getenv('LAJI_API_URL')
        ranges1 = get_value_ranges(f"{laji_api_url}/metadata/ranges?lang=fi&asLookupObject=true&access_token={access_token}")
        ranges2 = get_enumerations(f"{laji_api_url}/warehouse/enumeration-labels?access_token={access_token}")
        value_ranges = ranges1 | ranges2

        # Call the function
        result_gdf = compute_all(self.gdf, value_ranges, self.collection_names, 'src/municipalities.geojson')

        # Test direct mappings
        self.assertEqual(result_gdf['unit.atlasClass'][0], 'Epätodennäköinen pesintä')
        self.assertEqual(result_gdf['unit.atlasCode'][0], '1 Epätodennäköinen pesintä: Havaittu pesimäaikaan lajin yksilö, mutta havainto ei viittaa pesintään kyseisessä atlasruudussa')
        self.assertEqual(result_gdf['unit.linkings.originalTaxon.primaryHabitat.habitat'][0], 'M – Metsät')
        self.assertEqual(result_gdf['unit.linkings.originalTaxon.latestRedListStatusFinland.status'][0], 'EX – Sukupuuttoon kuolleet')
        self.assertEqual(result_gdf['unit.linkings.taxon.threatenedStatus'][0], 'Lakisääteinen')
        self.assertEqual(result_gdf['unit.recordBasis'][0], 'Näyte')
        self.assertEqual(result_gdf['unit.interpretations.recordQuality'][0], 'Asiantuntijan varmistama')
        self.assertEqual(result_gdf['document.secureReasons'][0], 'Lajitiedon sensitiivisyys')
        self.assertEqual(result_gdf['unit.sex'][0], 'koiras')
        self.assertEqual(result_gdf['unit.abundanceUnit'][0], 'Yksilömäärä')
        self.assertEqual(result_gdf['document.linkings.collectionQuality'][0], 'Ammattiaineistot / asiantuntijoiden laadunvarmistama')

        # Test mappings with multiple values
        self.assertEqual(result_gdf['unit.linkings.originalTaxon.administrativeStatuses'][0], 'Uhanalaiset lajit (LSA 2023/1066, liite 6), Erityisesti suojeltavat lajit (LSA 2023/1066, liite 6)')

        # Test computed values
        self.assertEqual(result_gdf['compute_from_individual_count'][0], 'paikalla')
        self.assertEqual(result_gdf['compute_from_individual_count'][1], 'poissa')
        self.assertEqual(result_gdf['compute_from_collection_id'][0], 'Collection 1')
        self.assertEqual(result_gdf['compute_from_collection_id'][1], 'Collection 2')

        # Test municipality and ELY area computations
        self.assertEqual(result_gdf['computed_municipality'][0], 'Helsinki')
        self.assertEqual(result_gdf['computed_ely_area'][0], 'Uudenmaan ELY-keskus')
        self.assertEqual(result_gdf['computed_municipality'][1], 'Hämeenlinna')
        self.assertEqual(result_gdf['computed_ely_area'][1], 'Hämeen ELY-keskus')

        # Test local ID generation
        self.assertEqual(result_gdf['Paikallinen_tunniste'][0], '1')

class TestGetBiogeographicalRegionFromId(unittest.TestCase):
    
    def test_valid_ids(self):
        """
        Test valid IDs from the id_mapping dictionary.
        """
        self.assertEqual(get_biogeographical_region_from_id("ML.251"), "ahvenanmaa")
        self.assertEqual(get_biogeographical_region_from_id("ML.252"), "varsinais_suomi")
        self.assertEqual(get_biogeographical_region_from_id("ML.270"), "enontekion_lappi")
        self.assertEqual(get_biogeographical_region_from_id("ML.264"), "kainuu")

    def test_none_id(self):
        """
        Test with None as input.
        """
        self.assertEqual(get_biogeographical_region_from_id(None), "empty_biogeographical_region")
    
class TestGetTitleNameFromTableName(unittest.TestCase):
    
    def test_valid_table_name(self):
        """
        Test a valid table name that exists in the table_mapping.
        """
        self.assertEqual(get_title_name_from_table_name("sompion_lappi_polygons"), "Sompion Lappi")
        self.assertEqual(get_title_name_from_table_name("kittilan_lappi_lines"), "Kittilän Lappi")
        self.assertEqual(get_title_name_from_table_name("pohjois_karjala_points"), "Pohjois-Karjala")
    
    def test_unknown_table_name(self):
        """
        Test a table name that doesn't exist in the table_mapping.
        """
        self.assertEqual(get_title_name_from_table_name("unknown_area_polygons"), "Finland")
    
class TestMapValues(unittest.TestCase):

    def setUp(self):
        load_dotenv()
        access_token = os.getenv('ACCESS_TOKEN')
        laji_api_url = os.getenv('LAJI_API_URL')
        value_ranges_url = f'{laji_api_url}/metadata/ranges?lang=fi&asLookupObject=true&access_token={access_token}'
        self.value_ranges = get_value_ranges(value_ranges_url)

    def test_all_mapped_values(self):
        # Test when all values are mapped
        col = pd.Series(['MX.regionallyThreatened2020_4d', 'http://tun.fi/MX.birdsDirectiveStatusAppendix2A'])
        expected_result = pd.Series(['Alueellisesti uhanalainen 2020 - 4d Pohjoisboreaalinen, Tunturi-Lappi', 'EU:n lintudirektiivin II/A-liite'])
        result = map_values(col, self.value_ranges)
        pd.testing.assert_series_equal(result, expected_result)

    def test_partial_mapped_values_and_nones(self):
        # Test when some values are mapped, some not
        col = pd.Series(['http://tun.fi/MX.gameBird', 'MX.gameMammal', 'xyz123'])
        expected_result = pd.Series(['Riistalintu (Metsästyslaki 1993/615)', 'Riistanisäkäs (Metsästyslaki 1993/615; 2019/683)', 'xyz123'])
        result = map_values(col, self.value_ranges)
        pd.testing.assert_series_equal(result, expected_result)

class TestComputeCollectionId(unittest.TestCase):

    def setUp(self):
        # Sample dictionary for collection names
        self.collection_names = {
            'HR.3553': 'Collection A',
            'HR.1245': 'Collection B',
            'HR.9999': 'Collection C'
        }

    def test_all_mapped_ids(self):
        # Test when all collection ids have corresponding names
        collection_id_col = pd.Series(['http://tun.fi/HR.3553', 'http://tun.fi/HR.1245'])
        expected_result = pd.Series(['Collection A', 'Collection B'])
        result = compute_collection_id(collection_id_col, self.collection_names)
        pd.testing.assert_series_equal(result, expected_result)

    def test_partial_mapped_ids(self):
        # Test when some collection ids are missing in the dictionary
        collection_id_col = pd.Series(['http://tun.fi/HR.3553', 'http://tun.fi/HR.0000'])
        expected_result = pd.Series(['Collection A', None])  # HR.0000 is not mapped
        result = compute_collection_id(collection_id_col, self.collection_names)
        pd.testing.assert_series_equal(result, expected_result)

    def test_mixed_ids(self):
        # Test when ids contain URLs that should be stripped
        collection_id_col = pd.Series(['http://tun.fi/HR.3553', 'HR.1245', '', None])
        expected_result = pd.Series(['Collection A', 'Collection B', None, None])
        result = compute_collection_id(collection_id_col, self.collection_names)
        pd.testing.assert_series_equal(result, expected_result)

class TestComputeIndividualCount(unittest.TestCase):
    def test_all_mapped_ids(self):
        collection_id_col = pd.Series([0, 1, 10])
        expected_result = pd.Series(['poissa', 'paikalla', 'paikalla'])
        result = pd.Series(compute_individual_count(collection_id_col))
        pd.testing.assert_series_equal(result, expected_result)

if __name__ == '__main__':
    unittest.main()