import unittest
import os, sys
from tempfile import TemporaryDirectory
from datetime import date
from unittest.mock import mock_open, patch

sys.path.append('src/')

from edit_config import add_to_pygeoapi_config, clear_collections_from_config, add_metadata_to_config

class TestClearCollectionsFromConfig(unittest.TestCase):

    def setUp(self):
        # Create temporary directory for testing
        self.temp_dir = TemporaryDirectory()
        self.pygeoapi_config = os.path.join(self.temp_dir.name, "pygeoapi_config.txt")
        self.pygeoapi_config_out = os.path.join(self.temp_dir.name, "pygeoapi_config_out.txt")
        self.date = str(date.today())

        # Create input configuration file with collections
        with open(self.pygeoapi_config, "w") as config_file:
            config_file.write("blabla metadata and server info blabla \n")
            config_file.write("blabla updated 2024-01-01 yes yes \n")
            config_file.write("resources:\n")
            config_file.write("  - name: collection1\n")
            config_file.write("    type: feature\n")
            config_file.write("    data: data/collection1.json\n")
            config_file.write("  - name: collection2\n")
            config_file.write("    type: feature\n")
            config_file.write("    data: data/collection2.json\n")

    def test_clear_collections_from_config(self):
        # Call the function being tested
        clear_collections_from_config(self.pygeoapi_config, self.pygeoapi_config_out)

        # Assert that output config file exists
        self.assertTrue(os.path.exists(self.pygeoapi_config))

        # Assert content of the output config file
        with open(self.pygeoapi_config_out, "r") as output_file:
            output_content = output_file.read()
            self.assertNotIn("collection1", output_content)
            self.assertNotIn("collection2", output_content)
            self.assertIn(self.date, output_content)

    def tearDown(self):
        # Cleanup: Close temporary directory
        self.temp_dir.cleanup()

class TestAddToPygeoapiConfig(unittest.TestCase):

    def setUp(self):
        # Create temporary directory for testing
        self.temp_dir = TemporaryDirectory()
        self.template_path = os.path.join(self.temp_dir.name, "template.txt")
        self.config_path = os.path.join(self.temp_dir.name, "output_config.txt")

        # Create template file with placeholders
        with open(self.template_path, "w") as template_file:
            template_file.write("Hello, name! (this should change)")

        with open(self.config_path, "w") as output_config:
            output_config.write("Hello, name! (this should remain)\n\n")

    def test_add_to_pygeoapi_config(self):
        # Prepare template params
        template_params = {"name": "world"}

        # Call the function being tested
        add_to_pygeoapi_config(self.template_path, template_params, self.config_path)

        # Assert that output config file exists
        self.assertTrue(os.path.exists(self.config_path))

        # Assert content of the output config file
        with open(self.config_path, "r") as output_file:
            output_content = output_file.read()
            self.assertIn("Hello, world!", output_content)

    def tearDown(self):
        # Cleanup: Close temporary directory
        self.temp_dir.cleanup()

class TestAddMetadataToConfig(unittest.TestCase):

    @patch("builtins.open", new_callable=mock_open)
    def test_metadata_addition(self, mock_file):
        # Inputs for the function
        pygeoapi_config_out = "fake_path_to_config.yml"
        db_path_in_config = "path_to_tinydb"

        # Expected date (current date)
        expected_date = str(date.today())

        # Expected content that should be written to the file
        expected_content = f"""
    occurrence-metadata:
        type: collection
        title: Occurrence Metadata 
        description: This metadata record contains metadata of the all collections in this service 
        keywords:
            en:
                - metadata
                - record
        extents:
            spatial:
                bbox: [19.08317359,59.45414258,31.58672881,70.09229553]
                crs: https://www.opengis.net/def/crs/EPSG/0/3067
            temporal: 
                begin: 1990-01-01T00:00:00Z
                end: {expected_date}T00:00:00Z
        providers:
          - type: record
            name: TinyDBCatalogue
            data: {db_path_in_config}
            id_field: externalId
            time_field: recordCreated
            title_field: title
    """

        # Call the function
        add_metadata_to_config(pygeoapi_config_out, db_path_in_config)

        # Assert that the file was opened in append mode
        mock_file.assert_called_once_with(pygeoapi_config_out, "a")

        # Assert that the correct content was written to the file
        mock_file().write.assert_called_once_with(expected_content)
        
if __name__ == "__main__":
    unittest.main()
