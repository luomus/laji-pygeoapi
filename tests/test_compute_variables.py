import unittest
import requests
import os, sys
import pandas as pd
import numpy as np
from shapely.geometry import Point, Polygon
import geopandas as gpd
import pandas as pd
import unittest

sys.path.append('src/')

from compute_variables import compute_areas, compute_collection_id, compute_individual_count, compute_all, map_values

class TestFunctions(unittest.TestCase):

    def test_compute_individual_count(self):
        # Test when individual_count_col has positive values
        individual_count_col = pd.Series([1, 5, 10])
        expected_output = np.array(['paikalla', 'paikalla', 'paikalla'])
        np.testing.assert_array_equal(compute_individual_count(individual_count_col), expected_output)

        # Test when individual_count_col has zero and negative values
        individual_count_col = pd.Series([0, -1, 0])
        expected_output = np.array(['poissa', 'poissa', 'poissa'])
        np.testing.assert_array_equal(compute_individual_count(individual_count_col), expected_output)

        # Test mixed values
        individual_count_col = pd.Series([0, 1, -5, 10])
        expected_output = np.array(['poissa', 'paikalla', 'poissa', 'paikalla'])
        np.testing.assert_array_equal(compute_individual_count(individual_count_col), expected_output)

    def test_compute_collection_id(self):
        collection_id_col = pd.Series([
            'http://tun.fi/HR.3553',
            'http://tun.fi/HR.9876',
            'http://tun.fi/HR.1234'
        ])
        collection_names = {
            'HR.3553': 'Collection 1',
            'HR.9876': 'Collection 2'
        }

        expected_output = pd.Series(['Collection 1', 'Collection 2', np.nan])
        pd.testing.assert_series_equal(compute_collection_id(collection_id_col, collection_names), expected_output)

        # Test with an empty Series
        collection_id_col = pd.Series([])
        expected_output = pd.Series([])
        pd.testing.assert_series_equal(compute_collection_id(collection_id_col, collection_names), expected_output)

    def test_map_values(self):
        # Test with a cell containing multiple values
        cell = 'http://tun.fi/MX.regionallyThreatened2020_1a, http://tun.fi/MX.regionallyThreatened2020_1b'
        expected_output = 'Alueellisesti uhanalainen 2020 - 1a Hemiboreaalinen, Ahvenanmaa, Alueellisesti uhanalainen 2020 - 1b Hemiboreaalinen, Lounainen rannikkomaa'
        self.assertEqual(map_values(cell), expected_output)

        # Test with a cell containing a single value
        cell = 'http://tun.fi/MX.finlex160_1997_appendix1'
        expected_output = 'VANHA Kalalajit, joihin sovelletaan luonnonsuojelulakia (LSA 1997/160, liite 1)'
        self.assertEqual(map_values(cell), expected_output)

        # Test with a cell containing unmapped values
        cell = 'value3'
        expected_output = 'value3'
        self.assertEqual(map_values(cell), expected_output)

        # Test with a cell containing both mapped and unmapped values
        cell = 'http://tun.fi/MX.otherPlantPest, value3'
        expected_output = 'Muu kasvintuhooja, value3'
        self.assertEqual(map_values(cell), expected_output)

        # Test with an empty cell
        cell = ''
        expected_output = ''
        self.assertEqual(map_values(cell), expected_output)

class TestComputeAreas(unittest.TestCase):

    def setUp(self):
        # Create a simple GeoDataFrame with points
        self.gdf_with_geom_and_ids = gpd.GeoDataFrame({
            'id': [1, 2],
            'geometry': [
                Point(24.941, 60.169),  # In Helsinki
                Point(24.941, 61.0)     # In hämeenlinna
            ]
        }, crs="EPSG:4326")


    def test_compute_areas_within_municipality(self):
        # Use the municipal_gdf as input instead of a file path
        municipalities, elys = compute_areas(self.gdf_with_geom_and_ids, 'src/municipalities_and_elys.geojson')

        # Test the first point that falls within Helsinki
        self.assertEqual(municipalities[0], 'Helsinki')
        self.assertEqual(elys[0], 'Uudenmaan ELY-keskus')

        # Test the second point that falls outside the defined municipality
        self.assertEqual(municipalities[1], 'Hämeenlinna')
        self.assertEqual(elys[1], 'Hämeen ELY-keskus')

    def test_compute_areas_with_different_crs(self):
        # Change CRS of the gdf_with_geom_and_ids to test reprojection
        self.gdf_with_geom_and_ids = self.gdf_with_geom_and_ids.to_crs(epsg=32635)  # UTM zone 35N

        municipalities, elys = compute_areas(self.gdf_with_geom_and_ids, 'src/municipalities_and_elys.geojson')

        # Test the first point that falls within Helsinki
        self.assertEqual(municipalities[0], 'Helsinki')
        self.assertEqual(elys[0], 'Uudenmaan ELY-keskus')

        # Test the second point that falls within Hämeenlinna
        self.assertEqual(municipalities[1], 'Hämeenlinna')
        self.assertEqual(elys[1], 'Hämeen ELY-keskus')

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
            'unit.unitId': [1, 2],
            'geometry': [Point(24.941, 60.169), Point(24.941, 61.0)]
        }, crs="EPSG:4326")

        self.collection_names = {
            'HR.1234': 'Collection 1',
            'HR.5678': 'Collection 2'
        }

    def test_compute_all(self):
        # Call the function
        result_gdf = compute_all(self.gdf, self.collection_names, 'src/municipalities_and_elys.geojson')

        # Test direct mappings
        self.assertEqual(result_gdf['unit.atlasClass'][0], 'Epätodennäköinen pesintä')
        self.assertEqual(result_gdf['unit.atlasCode'][1], '2 Mahdollinen pesintä: yksittäinen lintu kerran, on sopivaa pesimäympäristöä.')

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

    def test_missing_values_handling(self):
        # Introduce NaNs in the data
        self.gdf.loc[0, 'unit.atlasClass'] = None
        self.gdf.loc[1, 'unit.interpretations.individualCount'] = None

        # Call the function
        result_gdf = compute_all(self.gdf, self.collection_names, 'src/municipalities_and_elys.geojson')

        # Check that NaNs are handled correctly
        self.assertEqual(result_gdf['unit.atlasClass'][0], 'nan')
        self.assertEqual(result_gdf['compute_from_individual_count'][1], 'nan')

    def test_no_duplicate_columns(self):
        # Ensure the result does not have duplicate columns
        result_gdf = compute_all(self.gdf, self.collection_names, 'src/municipalities_and_elys.geojson')
        duplicate_columns = result_gdf.columns[result_gdf.columns.duplicated()]
        self.assertEqual(len(duplicate_columns), 0)

if __name__ == '__main__':
    unittest.main()