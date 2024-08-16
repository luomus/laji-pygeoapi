import unittest
from tinydb import TinyDB, Query
from pathlib import Path
from datetime import datetime
import sys

sys.path.append('src/')

from edit_metadata import empty_metadata_db, create_metadata

class TestMetadataFunctions(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Setup before all tests."""
        cls.test_db_path = 'test_metadata_db.json'
        cls.metadata_dict = {
            'dataset_name': 'Test Dataset',
            'bbox': [24.5, 60.0, 25.0, 60.5],
            'min_date': '2024-01-01T00:00:00Z',
            'max_date': '2024-12-31T23:59:59Z',
            'no_of_occurrences': 100,
            'table_no': 1,
            'quality_dict': {
                'Ammattiaineistot / asiantuntijoiden laadunvarmistama': 50,
                'Asiantuntevat harrastajat / asiantuntijoiden laadunvarmistama': 30,
                'Kansalaishavaintoja / ei laadunvarmistusta': 20
            }
        }
    
    def test_empty_metadata_db(self):
        """Test that empty_metadata_db correctly drops all tables."""
        db = TinyDB(self.test_db_path)
        empty_metadata_db(self.test_db_path)
        self.assertEqual(len(db.all()), 0, "Database should be empty after dropping tables.")
        db.close()

    def test_create_metadata(self):
        """Test that create_metadata correctly inserts metadata."""
        create_metadata(self.metadata_dict, self.test_db_path)
        
        db = TinyDB(self.test_db_path)
        result = db.all()
        self.assertEqual(len(result), 1, "There should be one record in the database.")
        
        record = result[0]
        self.assertEqual(record['id'], "ID_1", "The ID should match the expected ID.")
        self.assertEqual(record['properties']['title'], 'Test Dataset', "The title should match the dataset name.")
        self.assertEqual(record['properties']['description'], 'This dataset has 100 occurrences from the area of Test Dataset. The occurrences have been collected between 2024-01-01 and 2024-12-31.', "The description should be correctly formatted.")
        
        db.close()

    @classmethod
    def tearDownClass(cls):
        """Cleanup after all tests."""
        if Path(cls.test_db_path).exists():
            Path(cls.test_db_path).unlink()

if __name__ == '__main__':
    unittest.main()
