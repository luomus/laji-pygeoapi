import unittest
import os, sys
from tempfile import TemporaryDirectory

sys.path.append('src/')

from edit_config import add_to_pygeoapi_config, clear_collections_from_config

class TestAddToPygeoapiConfig(unittest.TestCase):

    def setUp(self):
        # Create temporary directory for testing
        self.temp_dir = TemporaryDirectory()
        self.template_path = os.path.join(self.temp_dir.name, "template.txt")
        self.output_config_path = os.path.join(self.temp_dir.name, "output_config.txt")

        # Create template file with placeholders
        with open(self.template_path, "w") as template_file:
            template_file.write("Hello, name! (this should change)")

        with open(self.output_config_path, "w") as output_config:
            output_config.write("Hello, name! (this should remain)\n\n")

    def test_add_to_pygeoapi_config(self):
        # Prepare template params
        template_params = {"name": "world"}

        # Call the function being tested
        add_to_pygeoapi_config(self.template_path, template_params, self.output_config_path)

        # Assert that output config file exists
        self.assertTrue(os.path.exists(self.output_config_path))

        # Assert content of the output config file
        with open(self.output_config_path, "r") as output_file:
            output_content = output_file.read()
            self.assertIn("Hello, world!", output_content)

    def tearDown(self):
        # Cleanup: Close temporary directory
        self.temp_dir.cleanup()

class TestClearCollectionsFromConfig(unittest.TestCase):

    def setUp(self):
        # Create temporary directory for testing
        self.temp_dir = TemporaryDirectory()
        self.pygeoapi_config_in = os.path.join(self.temp_dir.name, "pygeoapi_config_in.txt")
        self.pygeoapi_config_out = os.path.join(self.temp_dir.name, "pygeoapi_config_out.txt")

        # Create input configuration file with collections
        with open(self.pygeoapi_config_in, "w") as config_file:
            config_file.write("blabla metadata and server info blabla \n")
            config_file.write("resources:\n")
            config_file.write("  - name: collection1\n")
            config_file.write("    type: feature\n")
            config_file.write("    data: data/collection1.json\n")
            config_file.write("  - name: collection2\n")
            config_file.write("    type: feature\n")
            config_file.write("    data: data/collection2.json\n")

    def test_clear_collections_from_config(self):
        # Call the function being tested
        clear_collections_from_config(self.pygeoapi_config_in, self.pygeoapi_config_out)

        # Assert that output config file exists
        self.assertTrue(os.path.exists(self.pygeoapi_config_out))

        # Assert content of the output config file
        with open(self.pygeoapi_config_out, "r") as output_file:
            output_content = output_file.read()
            self.assertNotIn("collection1", output_content)
            self.assertNotIn("collection2", output_content)

    def tearDown(self):
        # Cleanup: Close temporary directory
        self.temp_dir.cleanup()

if __name__ == "__main__":
    unittest.main()
