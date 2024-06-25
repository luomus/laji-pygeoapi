import unittest
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import sys
from pandas.testing import assert_frame_equal

sys.path.append('src/')

from process_data import merge_taxonomy_data, get_min_max_dates, clean_table_name, translate_column_names, combine_similar_columns

class TestMergeTaxonomyData(unittest.TestCase):

    def setUp(self):
        # Sample occurrence data
        data_occurrence = {
            'unit.linkings.taxon.id': ["http://tun.fi/MX.26280", "http://tun.fi/MX.27899", "http://tun.fi/MX.27801", "http://tun.fi/MX.27800"],
            'some_other_column': [1, 2, 3, 4],
            'geometry': [Point(1, 1), Point(2, 2), Point(3, 3), Point(4, 4)]
        }
        self.occurrence_gdf = gpd.GeoDataFrame(data_occurrence, geometry='geometry')

        # Sample taxonomy data
        data_taxonomy = {
            'idMainTaxon': ["MX.26280", 'MX.27801', 'MX.27800'],
            'taxon_name': ['Taxon A', 'Taxon B', 'Taxon C']
        }
        self.taxonomy_df = pd.DataFrame(data_taxonomy)

    def test_successful_merge(self):
        """
        Test that the function correctly merges taxonomy data with the occurrence data.
        """
        merged_gdf = merge_taxonomy_data(self.occurrence_gdf, self.taxonomy_df)
        # Check the shape of the merged dataframe
        self.assertEqual(merged_gdf.shape[1], self.occurrence_gdf.shape[1]+2)
        self.assertEqual(merged_gdf.shape[0], self.occurrence_gdf.shape[0])

        # Check that the merge added the taxon_name column correctly
        self.assertIn('taxon_name', merged_gdf.columns)
        self.assertEqual(merged_gdf.loc[0, 'taxon_name'], 'Taxon A')
        self.assertEqual(merged_gdf.loc[2, 'taxon_name'], 'Taxon B')
        self.assertEqual(merged_gdf.loc[3, 'taxon_name'], 'Taxon C')
        self.assertTrue(pd.isna(merged_gdf.loc[1, 'taxon_name']))

    def test_no_matching_taxon_ids(self):
        """
        Test that the function correctly handles cases where no taxon IDs match.
        """
        # Create a taxonomy dataframe with no matching IDs
        data_taxonomy_no_match = {
            'idMainTaxon': ["http://tun.fi/MX.77777", "http://tun.fi/MX.8888", "http://tun.fi/MX.999"],
            'taxon_name': ['Taxon X', 'Taxon Y', 'Taxon Z']
        }
        taxonomy_df_no_match = pd.DataFrame(data_taxonomy_no_match)
        merged_gdf = merge_taxonomy_data(self.occurrence_gdf, taxonomy_df_no_match)

        # Check that the taxon_name column is all NaN
        self.assertIn('taxon_name', merged_gdf.columns)
        self.assertTrue(merged_gdf['taxon_name'].isna().all())

    def test_partial_matching_taxon_ids(self):
        """
        Test that the function correctly handles cases where some taxon IDs match.
        """
        data_occurrence_partial = {
            'unit.linkings.taxon.id': ["http://tun.fi/MX.26280", "http://tun.fi/MX.33333", "http://tun.fi/MX.27801", "http://tun.fi/MX.00000"],
            'some_other_column': [1, 2, 3, 4],
            'geometry': [Point(1, 1), Point(2, 2), Point(3, 3), Point(4, 4)]
        }
        occurrence_gdf_partial = gpd.GeoDataFrame(data_occurrence_partial, geometry='geometry')
        merged_gdf = merge_taxonomy_data(occurrence_gdf_partial, self.taxonomy_df)

        # Check that the taxon_name column is correctly merged
        self.assertIn('taxon_name', merged_gdf.columns)
        self.assertEqual(merged_gdf.loc[0, 'taxon_name'], 'Taxon A')
        self.assertTrue(pd.isna(merged_gdf.loc[1, 'taxon_name']))
        self.assertEqual(merged_gdf.loc[2, 'taxon_name'], 'Taxon B')
        self.assertTrue(pd.isna(merged_gdf.loc[3, 'taxon_name']))

    def test_missing_taxon_id_column(self):
        """
        Test that the function handles cases where the 'unit.linkings.taxon.id' column is missing.
        """
        occurrence_gdf_no_id = self.occurrence_gdf.drop(columns=['unit.linkings.taxon.id'])
        with self.assertRaises(KeyError):
            merge_taxonomy_data(occurrence_gdf_no_id, self.taxonomy_df)

    def test_empty_occurrence_gdf(self):
        """
        Test that the function handles an empty occurrence GeoDataFrame.
        """
        empty_gdf = self.occurrence_gdf.iloc[0:0]
        merged_gdf = merge_taxonomy_data(empty_gdf, self.taxonomy_df)
        
        # Check that the returned GeoDataFrame is still empty
        self.assertTrue(merged_gdf.empty)


class TestCombineSimilarColumns(unittest.TestCase):
    
    def setUp(self):
        # Create sample data for testing
        data = {
            'keyword[0]': ['a', 'b', 'c'],
            'keyword[1]': ['d', 'e', 'f'],
            'other_col': [7, 8, 9],
            'keyword2[0]': ['g', 'h', 'i'],
            'keyword2[1]': ['j', 'k', 'l']
        }
        self.gdf = gpd.GeoDataFrame(data)

    def test_combine_similar_columns(self):
        expected_data = {
            'other_col': [7, 8, 9],
            'keyword': ['a, d', 'b, e', 'c, f'],
            'keyword2': ['g, j', 'h, k', 'i, l']
        }
        expected_gdf = gpd.GeoDataFrame(expected_data)
        result_gdf = combine_similar_columns(self.gdf)
        assert_frame_equal(result_gdf, expected_gdf)

    def test_no_similar_columns(self):
        data = {
            'col1': [1, 2, 3],
            'col2': [4, 5, 6],
            'col3': [7, 8, 9]
        }
        gdf = gpd.GeoDataFrame(data)
        result_gdf = combine_similar_columns(gdf)
        assert_frame_equal(result_gdf, gdf)

    def test_mixed_columns(self):
        data = {
            'keyword[0]': ['a', 'b', 'c'],
            'keyword[1]': ['d', 'e', 'f'],
            'col1': [7, 8, 9],
            'col2': [10, 11, 12]
        }
        gdf = gpd.GeoDataFrame(data)
        expected_data = {
            'col1': [7, 8, 9],
            'col2': [10, 11, 12],
            'keyword': ['a, d', 'b, e', 'c, f']
        }
        expected_gdf = gpd.GeoDataFrame(expected_data)
        result_gdf = combine_similar_columns(gdf)
        assert_frame_equal(result_gdf, expected_gdf)

    def test_empty_dataframe(self):
        gdf = gpd.GeoDataFrame()
        result_gdf = combine_similar_columns(gdf)
        assert_frame_equal(result_gdf, gdf)


class TestGetMinMaxDates(unittest.TestCase):
    
    def setUp(self):
        # Create sample data for testing
        data = {
            'Keruu_aloitus_pvm': ['2023-01-01', '2023-02-11', '2023-03-25'],
            'Keruu_lopetus_pvm': ['2023-04-01', '2023-05-01', '2025-06-01']
        }
        self.gdf = gpd.GeoDataFrame(data)

    def test_get_min_max_dates(self):
        first_date, end_date = get_min_max_dates(self.gdf)
        self.assertEqual(first_date, '2023-01-01T00:00:00Z')
        self.assertEqual(end_date, '2025-06-01T00:00:00Z')

    def test_with_missing_start_dates(self):
        data = {
            'Keruu_aloitus_pvm': [pd.NaT, '2023-02-01', '2023-03-01'],
            'Keruu_lopetus_pvm': ['2023-04-01', '2023-05-01', '2023-06-01']
        }
        gdf = gpd.GeoDataFrame(data)
        first_date, end_date = get_min_max_dates(gdf)
        self.assertEqual(first_date, '2023-02-01T00:00:00Z')
        self.assertEqual(end_date, '2023-06-01T00:00:00Z')

    def test_with_all_missing_dates(self):
        data = {
            'Keruu_aloitus_pvm': [pd.NaT, pd.NaT, pd.NaT],
            'Keruu_lopetus_pvm': [pd.NaT, pd.NaT, pd.NaT]
        }
        gdf = gpd.GeoDataFrame(data)
        first_date, end_date = get_min_max_dates(gdf)
        self.assertIsNone(first_date)
        self.assertIsNone(end_date)

    def test_with_empty_dataframe(self):
        gdf = gpd.GeoDataFrame(columns=['Keruu_aloitus_pvm', 'Keruu_lopetus_pvm'])
        first_date, end_date = get_min_max_dates(gdf)
        self.assertIsNone(first_date)
        self.assertIsNone(end_date)

    def test_with_invalid_dates(self):
        data = {
            'Keruu_aloitus_pvm': ['invalid_date', '2023-02-01', '2023-03-01'],
            'Keruu_lopetus_pvm': ['2023-04-01', 'invalid_date', '2023-06-01']
        }
        gdf = gpd.GeoDataFrame(data)
        with self.assertRaises(ValueError):
            get_min_max_dates(gdf)


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

class TestTranslateColumnNames(unittest.TestCase):

    def setUp(self):
        # Create a sample GeoDataFrame
        data = {'gathering.gatheringId': [1, 2, 3], 'unit.sex': [4, 5, 6]}
        self.gdf = pd.DataFrame(data)

        # Create a sample lookup table DataFrame and save it to a CSV file
        lookup_data = {'finbif_api_var': ['gathering.gatheringId', 'unit.sex'], 
                       'virva': ['Keruutapahtuman_tunniste', 'Sukupuoli']}
        self.lookup_table = pd.DataFrame(lookup_data)
        self.lookup_table_path = 'test_lookup_table.csv'
        self.lookup_table.to_csv(self.lookup_table_path, sep=';', index=False)

    def tearDown(self):
        # Clean up the created CSV file after the test
        import os
        if os.path.exists(self.lookup_table_path):
            os.remove(self.lookup_table_path)

    def test_translate_column_names_with_valid_input(self):
        # Test with valid input
        expected_result = pd.DataFrame({'Keruutapahtuman_tunniste': [1, 2, 3], 'Sukupuoli': [4, 5, 6]})
        self.assertTrue(translate_column_names(self.gdf, self.lookup_table_path).equals(expected_result))

    def test_translate_column_names_with_empty_lookup_table(self):
        # Test with an empty lookup table
        empty_lookup_table = pd.DataFrame(columns=['finbif_api_var', 'virva'])
        with self.assertRaises(Exception):
            translate_column_names(self.gdf, empty_lookup_table)

if __name__ == '__main__':
    unittest.main()