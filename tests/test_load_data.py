import unittest, sys
import geopandas as gpd
import pandas as pd
import os
from dotenv import load_dotenv
from unittest.mock import patch, MagicMock
import requests

sys.path.append('src/')

from load_data import get_last_page, download_page, get_occurrence_data, find_main_taxon, get_taxon_data, get_value_ranges, get_collection_names, fetch_json_with_retry

class TestGetLastPage(unittest.TestCase):

    def test_get_last_page_valid(self):
        # Test with a valid URL
        url = "https://laji.fi/api/warehouse/query/unit/list?page=1&pageSize=1&geoJSON=true&featureType=ORIGINAL_FEATURE"
        last_page = get_last_page(url)
        self.assertIsInstance(last_page, int)
        self.assertGreater(last_page, 400)

class TestGetCollectionNames(unittest.TestCase):

    @patch('load_data.fetch_json_with_retry')
    def test_successful_fetch(self, mock_fetch):
        """
        Test that the function returns the correct dictionary when valid data is returned.
        """
        mock_fetch.return_value = {
            'results': [
                {'id': '1', 'longName': 'Collection One'},
                {'id': '2', 'longName': 'Collection Two'}
            ]
        }

        result = get_collection_names("http://example.com/api")
        expected = {'1': 'Collection One', '2': 'Collection Two'}
        self.assertEqual(result, expected)
        mock_fetch.assert_called_once_with("http://example.com/api")

class TestFetchJsonWithRetry(unittest.TestCase):
    
    @patch('requests.get')
    def test_successful_fetch(self, mock_get):
        """
        Test that the function successfully fetches JSON on the first attempt.
        """
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"key": "value"}
        mock_get.return_value = mock_response
        
        result = fetch_json_with_retry("http://example.com/api")
        self.assertEqual(result, {"key": "value"})
        mock_get.assert_called_once_with("http://example.com/api")

    @patch('requests.get')
    def test_retry_on_failure(self, mock_get):
        """
        Test that the function retries on failure and succeeds within max_retries.
        """
        # Simulate the first attempt raising an exception, then a successful response
        mock_get.side_effect = [requests.exceptions.RequestException, MagicMock(status_code=200, json=lambda: {"key": "value"})]
        
        result = fetch_json_with_retry("http://example.com/api", max_retries=3, delay=1)
        self.assertEqual(result, {"key": "value"})
        self.assertEqual(mock_get.call_count, 2)

    @patch('requests.get')
    def test_retries_exceed_max_retries(self, mock_get):
        """
        Test that the function returns None after max_retries are exhausted.
        """
        # Simulate a repeated failure for all attempts
        mock_get.side_effect = requests.exceptions.RequestException
        
        result = fetch_json_with_retry("http://example.com/api", max_retries=3, delay=1)
        self.assertIsNone(result)
        self.assertEqual(mock_get.call_count, 3)

class TestDownloadPage(unittest.TestCase):

    def test_download_page(self):
        # Test with a valid url and page_no
        url = "https://laji.fi/api/warehouse/query/unit/list?page=1&pageSize=1&geoJSON=true&featureType=ORIGINAL_FEATURE"
        gdf = download_page(url, page_no=1)
        self.assertTrue(isinstance(gdf, gpd.GeoDataFrame))
        self.assertFalse(gdf.empty)
        # more assertions?

class TestGetOccurrenceData(unittest.TestCase):

    def test_get_occurrence_data(self):
        url = "https://laji.fi/api/warehouse/query/unit/list?page=1&pageSize=1&geoJSON=true&featureType=ORIGINAL_FEATURE"

        # Test when multiprocessing is True
        gdf = get_occurrence_data(url=url, startpage=1, endpage=2, multiprocessing=True)
        self.assertTrue(isinstance(gdf, gpd.GeoDataFrame))
        self.assertFalse(gdf.empty)
        self.assertEqual(gdf['geometry'].dtype, 'geometry') 
        self.assertEqual(gdf['unit.unitId'].dtype, 'object')
    
        # Test when multiprocessing is False
        gdf2 = get_occurrence_data(url=url, startpage=1, endpage=2, multiprocessing=False)
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

        # Tests
        df = get_taxon_data(taxon_name_url)
        self.assertTrue(isinstance(df, pd.DataFrame))
        self.assertFalse(df.empty)

class TestFindMainTaxon(unittest.TestCase):
    
    def test_with_list(self):
        row = ['MVL.1', 'MVL.2', 'MVL.3']
        result = find_main_taxon(row)
        self.assertEqual(result, 'MVL.1')

if __name__ == "__main__":
    unittest.main()
