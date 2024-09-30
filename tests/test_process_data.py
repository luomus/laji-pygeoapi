import unittest
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon, LineString, GeometryCollection, MultiPolygon
import sys
from geopandas.testing import assert_geodataframe_equal

sys.path.append('src/')

from process_data import merge_taxonomy_data, translate_column_names, combine_similar_columns, convert_geometry_collection_to_multipolygon, merge_duplicates, process_facts, validate_geometry


class TestMergeTaxonomyData(unittest.TestCase):

    def setUp(self):
        # Sample occurrence data
        data_occurrence = {
            'unit.linkings.originalTaxon.informalTaxonGroups[0]': ["http://tun.fi/MVL.26280", "http://tun.fi/MVL.27899", "http://tun.fi/MVL.27801", "http://tun.fi/MVL.27800"],
            'some_other_column': [1, 2, 3, 4],
            'geometry': [Point(1, 1), Point(2, 2), Point(3, 3), Point(4, 4)]
        }
        self.occurrence_gdf = gpd.GeoDataFrame(data_occurrence, geometry='geometry')

        # Sample taxonomy data
        data_taxonomy = {
            'id': ["MVL.26280", 'MVL.27801', 'MVL.27800'],
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
            'id': ["http://tun.fi/MVL.77777", "http://tun.fi/MVL.8888", "http://tun.fi/MVL.999"],
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
            'unit.linkings.originalTaxon.informalTaxonGroups[0]': ["http://tun.fi/MVL.26280", "http://tun.fi/MVL.33333", "http://tun.fi/MVL.27801", "http://tun.fi/MVL.00000"],
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

class TestValidateGeometry(unittest.TestCase):

    def test_no_invalid_geometries(self):
        """
        Test when all geometries are valid.
        """
        gdf = gpd.GeoDataFrame({
            'geometry': [Point(0, 0), Point(1, 1), Point(2, 2)]
        })

        result_gdf, edited_count = validate_geometry(gdf)
        
        self.assertEqual(edited_count, 0)
        self.assertTrue(result_gdf.equals(gdf))

    def test_invalid_geometries_fixed(self):
        """
        Test when some geometries are invalid and some are fixed.
        """
        # Create an invalid self-intersecting polygon (bowtie shape)
        invalid_geom = Polygon([(0, 0), (2, 0), (0, 2), (2, 2), (0, 0)])

        gdf = gpd.GeoDataFrame({
            'geometry': [invalid_geom, Point(1, 1)]
        })

        result_gdf, edited_count = validate_geometry(gdf)
        
        # Ensure that the invalid geometry has been fixed
        self.assertEqual(edited_count, 1)
        self.assertTrue(result_gdf.is_valid.all())
    
    def test_mixed_geometries(self):
        """
        Test with a mix of valid and invalid geometries.
        """
        # Valid LineString, valid Point, invalid self-intersecting Polygon
        valid_line = LineString([(0, 0), (1, 1)])
        valid_point = Point(2, 2)
        invalid_polygon = Polygon([(0, 0), (2, 0), (0, 2), (2, 2), (0, 0)])

        gdf = gpd.GeoDataFrame({
            'geometry': [valid_line, valid_point, invalid_polygon]
        })

        result_gdf, edited_count = validate_geometry(gdf)
        
        self.assertEqual(edited_count, 1)  # Only the invalid polygon should be fixed
        self.assertTrue(result_gdf.is_valid.all())

class TestCombineSimilarColumns(unittest.TestCase):

    def test_combine_single_group_of_columns(self):
        # Test combining columns with a single group of similar columns
        gdf = gpd.GeoDataFrame({
            'keyword[0]': ['a', 'b', None],
            'keyword[1]': ['c', None, 'd'],
            'geometry': [None, None, None]  # Placeholder for geometry data
        })

        expected_gdf = gdf.drop(columns=['keyword[0]', 'keyword[1]']).copy()
        expected_gdf['keyword'] = ['a, c', 'b', 'd']

        result_gdf = combine_similar_columns(gdf)
        assert_geodataframe_equal(result_gdf, expected_gdf)

    def test_no_similar_columns(self):
        # Test with no similar columns present
        gdf = gpd.GeoDataFrame({
            'col1': ['x', 'y', 'z'],
            'col2': ['a', 'b', 'c'],
            'geometry': [None, None, None]
        })

        expected_gdf = gdf.copy()

        result_gdf = combine_similar_columns(gdf)
        assert_geodataframe_equal(result_gdf, expected_gdf)

    def test_multiple_groups_of_similar_columns(self):
        # Test combining multiple groups of similar columns
        gdf = gpd.GeoDataFrame({
            'keyword[0]': ['a', None, 'c'],
            'keyword[1]': [None, 'b', 'd'],
            'other[0]': ['1', None, '3'],
            'other[1]': ['2', '2', None],
            'geometry': [None, None, None]
        })

        expected_gdf = gdf.drop(columns=['keyword[0]', 'keyword[1]', 'other[0]', 'other[1]']).copy()
        expected_gdf['keyword'] = ['a', 'b', 'c, d']
        expected_gdf['other'] = ['1, 2', '2', '3']

        result_gdf = combine_similar_columns(gdf)
        assert_geodataframe_equal(result_gdf, expected_gdf)

    def test_mixed_nan_values(self):
        # Test combining columns where some rows have NaN values
        gdf = gpd.GeoDataFrame({
            'keyword[0]': ['a', None, 'c'],
            'keyword[1]': [None, 'b', None],
            'keyword[2]': [None, None, 'd'],
            'geometry': [None, None, None]
        })

        expected_gdf = gdf.drop(columns=['keyword[0]', 'keyword[1]', 'keyword[2]']).copy()
        expected_gdf['keyword'] = ['a', 'b', 'c, d']

        result_gdf = combine_similar_columns(gdf)
        assert_geodataframe_equal(result_gdf, expected_gdf)

class TestTranslateColumnNames(unittest.TestCase):

    def test_translate_column_names_virva(self):
        # Mocking the lookup table
        lookup_table = 'src/lookup_table_columns.csv'

        # Creating a sample GeoDataFrame
        gdf = gpd.GeoDataFrame({
            'unit.unitId': [1, 2, 3],
            'unit.linkings.taxon.scientificName': ['asd', 'asd1', 'asd2'],
            'unit.interpretations.individualCount': [0, 1, 2],
            'extra_column': ['x', 'y', 'z']
        })

        # Calling the function
        result_gdf = translate_column_names(gdf, lookup_table, style='virva')

        self.assertNotIn('extra_column', result_gdf.columns)
        self.assertIn('Havainnon_tunniste', result_gdf.columns)
        self.assertEqual(result_gdf['Yksilomaara_tulkittu'].dtype, 'int')
        self.assertGreater(len(result_gdf.columns), 50)

class TestConvertGeometryCollectionToMultipolygon(unittest.TestCase):

    def setUp(self):
        """Set up some test cases with GeoDataFrames containing GeometryCollections."""
        # Simple Point and LineString geometries
        self.point = Point(1, 1)
        self.line = LineString([(0, 0), (1, 1)])
        self.polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        
        # Create a GeoDataFrame with different GeometryCollections
        self.gdf = gpd.GeoDataFrame({
            'geometry': [
                GeometryCollection([self.point, self.line]),  # GeometryCollection with Point and LineString
                GeometryCollection([self.polygon]),           # GeometryCollection with a single Polygon
                GeometryCollection([self.point, self.polygon]) # GeometryCollection with Point and Polygon
            ]
        })

    def test_geometry_collection_with_point_and_line(self):
        """Test converting a GeometryCollection with Point and LineString."""
        gdf_converted = convert_geometry_collection_to_multipolygon(self.gdf.copy(), buffer_distance=0.5)
        
        # First geometry should be buffered Point and LineString, converted to MultiPolygon
        geom = gdf_converted.loc[0, 'geometry']
        self.assertTrue(isinstance(geom, MultiPolygon), "Should convert to MultiPolygon.")
    
    def test_geometry_collection_with_single_polygon(self):
        """Test converting a GeometryCollection with a single Polygon."""
        gdf_converted = convert_geometry_collection_to_multipolygon(self.gdf.copy())
        
        # Second geometry should remain as a MultiPolygon (with one polygon)
        geom = gdf_converted.loc[1, 'geometry']
        self.assertTrue(isinstance(geom, MultiPolygon), "Should convert single Polygon to MultiPolygon.")

    def test_geometry_collection_with_point_and_polygon(self):
        """Test converting a GeometryCollection with Point and Polygon."""
        gdf_converted = convert_geometry_collection_to_multipolygon(self.gdf.copy(), buffer_distance=0.5)
        
        # Third geometry should buffer the point and return a MultiPolygon with the original Polygon and the buffered Point
        geom = gdf_converted.loc[2, 'geometry']
        self.assertTrue(isinstance(geom, MultiPolygon), "Should be MultiPolygon combining Polygon and buffered Point.")

    def test_empty_geometry_collection(self):
        """Test converting an empty GeometryCollection."""
        empty_gdf = gpd.GeoDataFrame({'geometry': [GeometryCollection()]})
        gdf_converted = convert_geometry_collection_to_multipolygon(empty_gdf)
        
        # Geometry should be None
        geom = gdf_converted.loc[0, 'geometry']
        self.assertIsNone(geom, "Empty GeometryCollection should result in None.")

    def test_no_geometry_collection(self):
        """Test case where no GeometryCollection is present."""
        no_geom_gdf = gpd.GeoDataFrame({'geometry': [self.polygon]})
        gdf_converted = convert_geometry_collection_to_multipolygon(no_geom_gdf)
        
        # Geometry should remain unchanged (Polygon)
        geom = gdf_converted.loc[0, 'geometry']
        self.assertEqual(geom, self.polygon, "Polygon should remain unchanged.")

class TestMergeDuplicates(unittest.TestCase):

    def setUp(self):
        # Create a sample GeoDataFrame
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
        self.gdf = gpd.GeoDataFrame(data, crs="EPSG:4326")
                

    def test_merge_duplicates(self):
        # Mock lookup table
        lookup_table = r'src/lookup_table_columns.csv'

        # Call the function to merge duplicates
        merged_gdf, _ = merge_duplicates(self.gdf, lookup_table)
        
        # Check the resulting GeoDataFrame
        self.assertEqual(len(merged_gdf), 2)  # Expect 2 unique rows
        self.assertEqual(merged_gdf.loc[0, 'Yksilomaara_tulkittu'], 27)  # Check updated value
        self.assertEqual(merged_gdf.loc[0, 'Keruutapahtuman_tunniste'], 'K1, K2, K4')  # Check aggregated values
        self.assertEqual(merged_gdf.loc[0, 'Havainnon_tunniste'], 'H1, H2, H4')  # Check aggregated values
        self.assertEqual(merged_gdf.loc[1, 'Keruutapahtuman_tunniste'], 'K3')  # Check non-duplicate value
        self.assertEqual(merged_gdf.loc[1, 'Havainnon_tunniste'], 'H3')  # Check non-duplicate value

class TestGetFacts(unittest.TestCase):

    def test_with_fact_columns(self):
        # Creating a GeoDataFrame with fact columns and corresponding value columns

        gdf = gpd.GeoDataFrame({
            'gathering.facts[0].fact': ['Seurattava laji', 'Sijainnin tarkkuusluokka', 'Peittävyysprosentti'],
            'gathering.facts[0].value': ['Laji1', 'Tarkkuus1', '10%'],
            'gathering.facts[1].fact': ['Havainnon laatu', 'Havainnon määrän yksikkö', 'randon column'],
            'gathering.facts[1].value': ['Laatu1', 'Yksikkö1', None],
            'geometry': [None, None, None]
        })

        # Running the function
        result_gdf = process_facts(gdf)

        # Check that fact and value columns are dropped
        self.assertNotIn('gathering.facts[0].fact', result_gdf.columns)
        self.assertNotIn('gathering.facts[0].value', result_gdf.columns)
        self.assertNotIn('randon column', result_gdf.columns)

        # Check that the correct values are assigned to the new columns
        self.assertEqual(result_gdf['Seurattava laji'].iloc[0], 'Laji1')
        self.assertEqual(result_gdf['Sijainnin tarkkuusluokka'].iloc[1], 'Tarkkuus1')
        self.assertEqual(result_gdf['Peittävyysprosentti'].iloc[2], '10%')
        self.assertEqual(result_gdf['Havainnon laatu'].iloc[0], 'Laatu1')
        self.assertEqual(result_gdf['Havainnon määrän yksikkö'].iloc[1], 'Yksikkö1')

if __name__ == '__main__':
    unittest.main()