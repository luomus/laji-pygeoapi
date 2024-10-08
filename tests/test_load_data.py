import unittest, sys
import geopandas as gpd
import pandas as pd

sys.path.append('src/')

from load_data import get_last_page, download_page, get_occurrence_data, find_main_taxon, get_taxon_data

class TestGetLastPage(unittest.TestCase):

    def test_get_last_page_valid(self):
        # Test with a valid URL
        data_url = "https://laji.fi/api/warehouse/query/unit/list?administrativeStatusId=MX.finlex160_1997_appendix4_2021,MX.finlex160_1997_appendix4_specialInterest_2021,MX.finlex160_1997_appendix2a,MX.finlex160_1997_appendix2b,MX.finlex160_1997_appendix3a,MX.finlex160_1997_appendix3b,MX.finlex160_1997_appendix3c,MX.finlex160_1997_largeBirdsOfPrey,MX.habitatsDirectiveAnnexII,MX.habitatsDirectiveAnnexIV,MX.birdsDirectiveStatusAppendix1,MX.birdsDirectiveStatusMigratoryBirds&redListStatusId=MX.iucnCR,MX.iucnEN,MX.iucnVU,MX.iucnNT&countryId=ML.206&time=1990-01-01/&aggregateBy=gathering.conversions.wgs84Grid05.lat,gathering.conversions.wgs84Grid1.lon&onlyCount=false&individualCountMin=0&coordinateAccuracyMax=1000&page=1&pageSize=10000&taxonAdminFiltersOperator=OR&collectionAndRecordQuality=PROFESSIONAL:EXPERT_VERIFIED,COMMUNITY_VERIFIED,NEUTRAL,UNCERTAIN;HOBBYIST:EXPERT_VERIFIED,COMMUNITY_VERIFIED,NEUTRAL;AMATEUR:EXPERT_VERIFIED,COMMUNITY_VERIFIED;&geoJSON=true&featureType=ORIGINAL_FEATURE"
        last_page = get_last_page(data_url)
        self.assertIsInstance(last_page, int)
        self.assertGreater(last_page, 400)

    def test_get_last_page_invalid(self):
        # Test with an invalid URL
        data_url = "invalid_url"
        last_page = get_last_page(data_url)
        self.assertEqual(last_page, 1)


class TestDownloadPage(unittest.TestCase):

    def test_download_page(self):
        # Test with a valid data_url and page_no
        data_url = "https://laji.fi/api/warehouse/query/unit/list?administrativeStatusId=MX.finlex160_1997_appendix4_2021,MX.finlex160_1997_appendix4_specialInterest_2021,MX.finlex160_1997_appendix2a,MX.finlex160_1997_appendix2b,MX.finlex160_1997_appendix3a,MX.finlex160_1997_appendix3b,MX.finlex160_1997_appendix3c,MX.finlex160_1997_largeBirdsOfPrey,MX.habitatsDirectiveAnnexII,MX.habitatsDirectiveAnnexIV,MX.birdsDirectiveStatusAppendix1,MX.birdsDirectiveStatusMigratoryBirds&redListStatusId=MX.iucnCR,MX.iucnEN,MX.iucnVU,MX.iucnNT&countryId=ML.206&time=1990-01-01/&aggregateBy=gathering.conversions.wgs84Grid05.lat,gathering.conversions.wgs84Grid1.lon&onlyCount=false&individualCountMin=0&coordinateAccuracyMax=1000&page=1&pageSize=10000&taxonAdminFiltersOperator=OR&collectionAndRecordQuality=PROFESSIONAL:EXPERT_VERIFIED,COMMUNITY_VERIFIED,NEUTRAL,UNCERTAIN;HOBBYIST:EXPERT_VERIFIED,COMMUNITY_VERIFIED,NEUTRAL;AMATEUR:EXPERT_VERIFIED,COMMUNITY_VERIFIED;&geoJSON=true&featureType=ORIGINAL_FEATURE"
        gdf = download_page(data_url, page_no=1)
        self.assertTrue(isinstance(gdf, gpd.GeoDataFrame))
        self.assertFalse(gdf.empty)
        # more assertions?

class TestGetOccurrenceData(unittest.TestCase):

    def test_get_occurrence_data(self):
        data_url = "https://laji.fi/api/warehouse/query/unit/list?administrativeStatusId=MX.finlex160_1997_appendix4_2021,MX.finlex160_1997_appendix4_specialInterest_2021,MX.finlex160_1997_appendix2a,MX.finlex160_1997_appendix2b,MX.finlex160_1997_appendix3a,MX.finlex160_1997_appendix3b,MX.finlex160_1997_appendix3c,MX.finlex160_1997_largeBirdsOfPrey,MX.habitatsDirectiveAnnexII,MX.habitatsDirectiveAnnexIV,MX.birdsDirectiveStatusAppendix1,MX.birdsDirectiveStatusMigratoryBirds&redListStatusId=MX.iucnCR,MX.iucnEN,MX.iucnVU,MX.iucnNT&countryId=ML.206&time=1990-01-01/&aggregateBy=gathering.conversions.wgs84Grid05.lat,gathering.conversions.wgs84Grid1.lon&onlyCount=false&individualCountMin=0&coordinateAccuracyMax=1000&page=1&pageSize=10&taxonAdminFiltersOperator=OR&collectionAndRecordQuality=PROFESSIONAL:EXPERT_VERIFIED,COMMUNITY_VERIFIED,NEUTRAL,UNCERTAIN;HOBBYIST:EXPERT_VERIFIED,COMMUNITY_VERIFIED,NEUTRAL;AMATEUR:EXPERT_VERIFIED,COMMUNITY_VERIFIED;&geoJSON=true&featureType=ORIGINAL_FEATURE"

        # Test when multiprocessing is True
        gdf = get_occurrence_data(data_url=data_url, startpage=1, endpage=2, multiprocessing=True)
        self.assertTrue(isinstance(gdf, gpd.GeoDataFrame))
        self.assertFalse(gdf.empty)
        self.assertEqual(gdf['geometry'].dtype, 'geometry') 
        self.assertEqual(gdf['unit.unitId'].dtype, 'object')
    
        # Test when multiprocessing is False
        gdf2 = get_occurrence_data(data_url=data_url, startpage=1, endpage=2, multiprocessing=False)
        self.assertTrue(isinstance(gdf2, gpd.GeoDataFrame))
        self.assertFalse(gdf2.empty)
        self.assertEqual(gdf2['geometry'].dtype, 'geometry') 
        self.assertEqual(gdf2['unit.unitId'].dtype, 'object')

        # Test similarity
        self.assertEqual(gdf.crs, gdf2.crs)
        self.assertCountEqual(gdf.columns, gdf2.columns)

class TestGetTaxonData(unittest.TestCase):

    def test_get_taxon_data(self):
        # Test with valid taxon_id_url and taxon_name_url
        taxon_name_url = f'https://laji.fi/api/informal-taxon-groups?pageSize=1000'

        # Test with 2 pages
        df = get_taxon_data(taxon_name_url)
        self.assertTrue(isinstance(df, pd.DataFrame))
        self.assertFalse(df.empty)

class TestFindMainTaxon(unittest.TestCase):
    
    def test_with_list(self):
        row = ['MVL.1', 'MVL.2', 'MVL.3']
        result = find_main_taxon(row)
        self.assertEqual(result, 'MVL.1')

    def test_with_empty_list(self):
        row = []
        with self.assertRaises(ValueError): 
            find_main_taxon(row)

    def test_with_non_list(self):
        row = 'NonListString'
        result = find_main_taxon(row)
        self.assertEqual(result, 'NonListString')

    def test_with_single_element_list(self):
        row = ['MVL.10']
        result = find_main_taxon(row)
        self.assertEqual(result, 'MVL.10')

    def test_with_mixed_numbers(self):
        row = ['MVL.5', 'MVL.10', 'MVL.3', 'MVL.7']
        result = find_main_taxon(row)
        self.assertEqual(result, 'MVL.3')

    def test_with_non_numeric_parts(self):
        row = ['MVL.5a', 'MVL.10b', 'MVL.3c']
        with self.assertRaises(ValueError):
            find_main_taxon(row)

if __name__ == "__main__":
    unittest.main()
