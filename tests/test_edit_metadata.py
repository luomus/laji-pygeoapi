import os
from tinydb import TinyDB
from pathlib import Path
import sys

from pygeoapi.scripts import edit_metadata

# run with:
# python -m pytest tests/test_edit_metadata.py -v

TEST_DB_PATH = 'test_metadata_db.json'

metadata_dict = {
    'dataset_name': 'satakunta_points',
    'bbox': [24.5, 60.0, 25.0, 60.5],
    'min_date': '2024-01-01T00:00:00Z',
    'max_date': '2024-12-31T23:59:59Z',
    'no_of_occurrences': 100,
    'geom_type': 'point',
    'table_no': 1,
    'quality_dict': {
        'Ammattiaineistot / asiantuntijoiden laadunvarmistama': 50,
        'Asiantuntevat harrastajat / asiantuntijoiden laadunvarmistama': 30,
        'Kansalaishavaintoja / ei laadunvarmistusta': 20
    }
}

def setup_module(module):
    # Ensure test DB is clean before tests
    if Path(TEST_DB_PATH).exists():
        Path(TEST_DB_PATH).unlink()

def teardown_module(module):
    # Clean up test DB after tests
    if Path(TEST_DB_PATH).exists():
        Path(TEST_DB_PATH).unlink()

def test_empty_metadata_db():
    db = TinyDB(TEST_DB_PATH)
    db.insert({'foo': 'bar'})
    db.close()
    edit_metadata.empty_metadata_db(TEST_DB_PATH)
    db = TinyDB(TEST_DB_PATH)
    assert len(db.all()) == 0, "Database should be empty after dropping tables."
    db.close()

def test_add_JSON_metadata_to_DB():
    # Ensure the database is empty before adding metadata
    edit_metadata.empty_metadata_db(TEST_DB_PATH)
    # Add the metadata to the database
    edit_metadata.add_JSON_metadata_to_DB(metadata_dict, TEST_DB_PATH)
    # Check if the metadata was added correctly
    db = TinyDB(TEST_DB_PATH)
    records = db.all()
    assert len(records) == 1, "There should be one record in the database."
    record = records[0]
    assert record['id'] == "ID_1", "The ID should match the expected ID."
    assert record['properties']['title'] == 'satakunta_points', "The title should match the dataset name."
    assert record['properties']['description'].startswith('This dataset has 100 point occurrence features'), "The description should be correctly formatted."
    db.close()

def test_create_metadata(monkeypatch, tmp_path):
    """
    Test create_metadata by mocking dependencies and checking DB output.
    """
    # Prepare mocks for edit_db and compute_variables
    class DummyEditDB:
        @staticmethod
        def get_table_bbox(table_name):
            return [24.5, 60.0, 25.0, 60.5]
        @staticmethod
        def get_table_dates(table_name):
            return ('2024-01-01T00:00:00Z', '2024-12-31T23:59:59Z')
        @staticmethod
        def get_amount_of_occurrences(table_name):
            return 100
        @staticmethod
        def get_quality_frequency(table_name):
            return {
                'Ammattiaineistot / asiantuntijoiden laadunvarmistama': 50,
                'Asiantuntevat harrastajat / asiantuntijoiden laadunvarmistama': 30,
                'Kansalaishavaintoja / ei laadunvarmistusta': 20
            }
        @staticmethod
        def get_all_tables():
            return ['satakunta_points']

    class DummyEditConfig:
        @staticmethod
        def add_to_pygeoapi_config(template_resource, template_params, pygeoapi_config_out):
            # Do nothing for config file in test
            pass

    monkeypatch.setattr(edit_metadata, "edit_db", DummyEditDB)
    monkeypatch.setattr(edit_metadata, "edit_config", DummyEditConfig)

    # Call create_metadata
    edit_metadata.create_metadata(
        template_resource="dummy_template",
        metadata_db_path=TEST_DB_PATH,
        pygeoapi_config_out="dummy_config_out"
    )
    db = TinyDB(TEST_DB_PATH)
    records = db.all()
    assert len(records) == 1, "There should be one record in the database."
    record = records[0]
    assert record['properties']['title'] == 'satakunta_points', "The title should match the dataset name."
    assert record['id'] == "ID_0", "The ID should match the expected ID for table_no=0."
    assert record['properties']['description'].startswith('This dataset has 100'), "The description should be correctly formatted."
    db.close()