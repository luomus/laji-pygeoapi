import unittest
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import sys

sys.path.append('src/')

from process_data import get_min_and_max_dates, clean_table_name, column_names_to_dwc, get_bbox

class TestGetBbox(unittest.TestCase):

    def test_get_bbox(self):
        # Create a GeoDataFrame with some geometries
        data = {'geometry': [Point(1, 2), Point(3, 4), Point(5, 6)]}
        gdf = gpd.GeoDataFrame(data)

        # Calculate the bounding box using the function
        bbox = get_bbox(gdf)

        # Expected bounding box values
        expected_bbox = [1, 2, 5, 6]

        # Assert that the calculated bounding box matches the expected values
        self.assertEqual(bbox, expected_bbox)

class TestGetMinAndMaxDates(unittest.TestCase):

    def setUp(self):
        # Create sample data
        self.data_all_formats = {
            'eventDateTimeDisplay': [
                "2024-02-01",
                "2024-05-02 5:11",
                "2021-05-03 05:24",
                "2025-05-04 [12:00]",
                "2024-05-05 12:00:00",
                "2020-02-02 [15:01-15:04]",
                "2019-01-01 - 2023-31-12"
            ]
        }
        self.sub_gdf = pd.DataFrame(self.data_all_formats)

    def test_get_min_and_max_dates(self):
        # Call the function
        start_date, end_date, dates = get_min_and_max_dates(self.sub_gdf)
        
        # Check if start_date and end_date are strings
        self.assertIsInstance(start_date, str)
        self.assertIsInstance(end_date, str)

        # Check if dates is a pandas Series
        self.assertIsInstance(dates, pd.Series)

        if start_date and end_date:
            # Check if start_date is earlier than or equal to end_date
            self.assertLessEqual(start_date, end_date)

            # Check if start_date and end_date get correct values
            self.assertEqual(start_date, "2019-01-01T00:00:00Z")
            self.assertEqual(end_date, "2025-05-04T12:00:00Z")

            # Check if all dates are within the expected range
            self.assertTrue(all(dates >= pd.to_datetime(start_date)))
            self.assertTrue(all(dates <= pd.to_datetime(end_date)))

    def test_empty_dataframe(self):
        # Test scenario where the input DataFrame is empty
        empty_df = pd.DataFrame({'eventDateTimeDisplay': []})
        start_date, end_date, dates = get_min_and_max_dates(empty_df)
        self.assertIsNone(start_date)
        self.assertIsNone(end_date)
        self.assertTrue(dates.empty)

    def test_dataframe_with_invalid_format(self):
        # Test scenario where the input DataFrame contains invalid date range format
        invalid_df = pd.DataFrame({'eventDateTimeDisplay': ["2024/05/01", "1.1.2023", "1999.24.12"]})
        start_date, end_date, dates = get_min_and_max_dates(invalid_df)
        self.assertIsNone(start_date)
        self.assertIsNone(end_date)
        self.assertTrue(dates.isnull)

class TestCleanTableName(unittest.TestCase):

    def test_clean_table_name_with_valid_input(self):
        # Test with valid input
        group_name = "My_Group Name!123"
        self.assertEqual(clean_table_name(group_name), "My_Group_Name123")

    def test_clean_table_name_with_empty_input(self):
        # Test with empty input
        group_name = ""
        self.assertEqual(clean_table_name(group_name), "unclassified")

    def test_clean_table_name_with_None_input(self):
        # Test with None input
        group_name = None
        self.assertEqual(clean_table_name(group_name), "unclassified")

    def test_clean_table_name_with_nan_input(self):
        # Test with 'nan' input
        group_name = "nan"
        self.assertEqual(clean_table_name(group_name), "unclassified")

    def test_clean_table_name_with_long_input(self):
        # Test with long input
        group_name = "a" * 50
        self.assertEqual(len(clean_table_name(group_name)), 40)

class TestColumnNamesToDwc(unittest.TestCase):

    def setUp(self):
        # Create a sample GeoDataFrame
        data = {'gathering.gatheringId': [1, 2, 3], 'unit.reportedTaxonId': [4, 5, 6]}
        self.gdf = pd.DataFrame(data)

        # Create a sample lookup table DataFrame and save it to a CSV file
        lookup_data = {'finbif_api_var': ['gathering.gatheringId', 'unit.reportedTaxonId'], 
                       'dwc': ['eventID', 'verbatimTaxonID']}
        self.lookup_table = pd.DataFrame(lookup_data)
        self.lookup_table_path = 'test_lookup_table.csv'
        self.lookup_table.to_csv(self.lookup_table_path, sep=';', index=False)

    def tearDown(self):
        # Clean up the created CSV file after the test
        import os
        if os.path.exists(self.lookup_table_path):
            os.remove(self.lookup_table_path)

    def test_column_names_to_dwc_with_valid_input(self):
        # Test with valid input
        expected_result = pd.DataFrame({'eventID': [1, 2, 3], 'verbatimTaxonID': [4, 5, 6]})
        self.assertTrue(column_names_to_dwc(self.gdf, self.lookup_table_path).equals(expected_result))

    def test_column_names_to_dwc_with_empty_lookup_table(self):
        # Test with an empty lookup table
        empty_lookup_table = pd.DataFrame(columns=['finbif_api_var', 'dwc'])
        with self.assertRaises(Exception):
            column_names_to_dwc(self.gdf, empty_lookup_table)

if __name__ == '__main__':
    unittest.main()