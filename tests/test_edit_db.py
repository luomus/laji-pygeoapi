import os, sys
import pytest
import pandas as pd
from sqlalchemy import text, inspect, MetaData
from geoalchemy2 import Geometry
from datetime import date

sys.path.append('src/')

import edit_db

# Run with:
# docker-compose -f tests/docker-compose-test.yaml up -d
# python -m pytest tests/test_edit_db.py -v

@pytest.fixture(autouse=True)
def set_env_vars():
    os.environ['POSTGRES_DB'] = 'test_db'
    os.environ['POSTGRES_USER'] = 'test_user'
    os.environ['POSTGRES_PASSWORD'] = 'test_pw'
    os.environ['POSTGRES_HOST'] = 'localhost'
    os.environ['POSTGRES_PORT'] = '5433'
    edit_db._engine = None
    yield

@pytest.fixture
def engine():
    # Always get a fresh engine for each test
    edit_db._engine = None
    return edit_db.get_engine()

def create_test_table(engine, table_name, geom_type='POINT', extra_cols=''):
    with engine.connect() as conn:
        conn.execute(text('CREATE EXTENSION IF NOT EXISTS postgis;'))
        conn.execute(text(f'''
            CREATE TABLE "{table_name}" (
                id SERIAL PRIMARY KEY,
                "Havainnon_tunniste" TEXT,
                "Lataus_pvm" TIMESTAMP,
                "Aineiston_laatu" TEXT,
                "Keruu_aloitus_pvm" TIMESTAMP,
                "Keruu_lopetus_pvm" TIMESTAMP,
                "Kunta" TEXT,
                geometry Geometry({geom_type}, 4326)
                {extra_cols}
            );
        '''))
        conn.commit()

def drop_test_table(engine, table_name):
    with engine.connect() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;'))
        conn.commit()

def test_get_engine_singleton(engine):
    engine1 = edit_db.get_engine()
    engine2 = edit_db.get_engine()
    assert engine1 is engine2

def test_connect_to_db(engine):
    # Just check that the engine connects and postgis extension exists
    with engine.connect() as conn:
        result = conn.execute(text("SELECT extname FROM pg_extension WHERE extname='postgis';")).fetchone()
        assert result is not None

def test_get_and_update_last_update(engine):
    # Table should not exist at first
    with engine.connect() as conn:
        conn.execute(text('DROP TABLE IF EXISTS last_update;'))
        conn.commit()

    # First call: table is empty, should return None
    result = edit_db.get_and_update_last_update()
    assert result is None

     # Second call: now table has today's date, should return it
    today = date.today()
    result2 = edit_db.get_and_update_last_update()
    assert isinstance(result2, date)
    assert result2 == today

def test_drop_all_tables(engine):
    create_test_table(engine, 'table1')
    create_test_table(engine, 'table2')
    edit_db.drop_all_tables()
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    # Only postgis default tables should remain
    assert all(t in edit_db.postgis_default_tables for t in tables)

def test_drop_table(engine):
    create_test_table(engine, 'table1')
    create_test_table(engine, 'table2')
    edit_db.drop_table(['table1'])
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    assert 'table1' not in tables
    assert 'table2' in tables

def test_get_all_tables(engine):
    drop_test_table(engine, 'table1')
    create_test_table(engine, 'table1')
    tables = edit_db.get_all_tables()
    assert 'table1' in tables

def test_get_table_bbox(engine):
    drop_test_table(engine, 'bbox_table')
    with engine.connect() as conn:
        conn.execute(text('''
            CREATE TABLE "bbox_table" (
                id SERIAL PRIMARY KEY,
                geometry Geometry(Point, 4326)
            );
        '''))
        conn.execute(text('''
            INSERT INTO "bbox_table" (geometry) VALUES
            (ST_GeomFromText('POINT(1 2)', 4326)),
            (ST_GeomFromText('POINT(3 4)', 4326));
        '''))
        conn.commit()
    bbox = edit_db.get_table_bbox('bbox_table')
    assert bbox == [1.0, 2.0, 3.0, 4.0]
    drop_test_table(engine, 'bbox_table')

def test_get_quality_frequency(engine):
    drop_test_table(engine, 'quality_table')
    with engine.connect() as conn:
        conn.execute(text('''
            CREATE TABLE "quality_table" (
                id SERIAL PRIMARY KEY,
                "Aineiston_laatu" TEXT,
                geometry Geometry(Point, 4326)
            );
        '''))
        conn.execute(text('''
            INSERT INTO "quality_table" ("Aineiston_laatu", geometry) VALUES
            ('good', ST_GeomFromText('POINT(1 2)', 4326)),
            ('good', ST_GeomFromText('POINT(2 3)', 4326)),
            ('bad', ST_GeomFromText('POINT(3 4)', 4326));
        '''))
        conn.commit()
    freq = edit_db.get_quality_frequency('quality_table')
    assert freq['good'] == 66.67
    assert freq['bad'] == 33.33
    drop_test_table(engine, 'quality_table')

def test_get_table_dates(engine):
    drop_test_table(engine, 'date_table')
    with engine.connect() as conn:
        conn.execute(text('''
            CREATE TABLE "date_table" (
                id SERIAL PRIMARY KEY,
                "Keruu_aloitus_pvm" TIMESTAMP,
                "Keruu_lopetus_pvm" TIMESTAMP,
                geometry Geometry(Point, 4326)
            );
        '''))
        conn.execute(text('''
            INSERT INTO "date_table" ("Keruu_aloitus_pvm", "Keruu_lopetus_pvm", geometry) VALUES
            ('2020-01-01', '2020-12-31', ST_GeomFromText('POINT(1 2)', 4326)),
            ('2020-02-01', '2020-11-30', ST_GeomFromText('POINT(2 3)', 4326));
        '''))
        conn.commit()
    min_date, max_date = edit_db.get_table_dates('date_table')
    assert min_date and min_date.startswith('2020-01-01')
    assert max_date and max_date.startswith('2020-12-31')
    drop_test_table(engine, 'date_table')

def test_check_table_exists(engine):
    drop_test_table(engine, 'exists_table')
    create_test_table(engine, 'exists_table')
    assert edit_db.check_table_exists('exists_table')
    drop_test_table(engine, 'exists_table')
    assert not edit_db.check_table_exists('exists_table')

def test_get_amount_of_occurrences(engine):
    drop_test_table(engine, 'occ_table')
    create_test_table(engine, 'occ_table')
    with engine.connect() as conn:
        conn.execute(text('''
            INSERT INTO "occ_table" ("Havainnon_tunniste", geometry) VALUES
            ('a', ST_GeomFromText('POINT(1 2)', 4326)),
            ('b', ST_GeomFromText('POINT(2 3)', 4326));
        '''))
        conn.commit()
    count = edit_db.get_amount_of_occurrences('occ_table')
    assert count == 2
    drop_test_table(engine, 'occ_table')

def test_get_amount_of_all_occurrences(engine):
    drop_test_table(engine, 'table1')
    drop_test_table(engine, 'table2')
    create_test_table(engine, 'table1')
    create_test_table(engine, 'table2')
    with engine.connect() as conn:
        conn.execute(text('''
            INSERT INTO "table1" ("Havainnon_tunniste", geometry) VALUES
            ('a', ST_GeomFromText('POINT(1 2)', 4326));
        '''))
        conn.execute(text('''
            INSERT INTO "table2" ("Havainnon_tunniste", geometry) VALUES
            ('b', ST_GeomFromText('POINT(2 3)', 4326)),
            ('c', ST_GeomFromText('POINT(3 4)', 4326));
        '''))
        conn.commit()
    total = edit_db.get_amount_of_all_occurrences()
    assert total == 3
    drop_test_table(engine, 'table1')
    drop_test_table(engine, 'table2')

def test_to_db(engine):
    import geopandas as gpd
    from shapely.geometry import Point, LineString, Polygon
    # Prepare a GeoDataFrame with mixed geometry types
    gdf = gpd.GeoDataFrame({
        'Kunta': ['A', 'B', 'C'],
        'geometry': [
            Point(1, 2),
            LineString([(0, 0), (1, 1)]),
            Polygon([(0, 0), (1, 0), (1, 1), (0, 0)])
        ]
    }, geometry='geometry', crs='EPSG:4326')
    table_names = ['points', 'lines', 'polygons']
    for t in table_names:
        drop_test_table(engine, t)
    failed = edit_db.to_db(gdf, table_names)
    assert failed == 0
    # Check that each table has 1 row
    for t in table_names:
        count = edit_db.get_amount_of_occurrences(t)
        assert count == 1
        drop_test_table(engine, t)


def test_update_single_table_indexes(engine):
    drop_test_table(engine, 'idx_table')
    create_test_table(engine, 'idx_table')
    edit_db.update_single_table_indexes('idx_table')
    # Check that indexes exist
    with engine.connect() as conn:
        idx = conn.execute(text('''
            SELECT indexname FROM pg_indexes WHERE tablename = 'idx_table';
        ''')).fetchall()
        idx_names = [i[0] for i in idx]
        assert any('idx_idx_table_Kunta' in n for n in idx_names)
        assert any('idx_idx_table_geom' in n for n in idx_names)
    drop_test_table(engine, 'idx_table')

def test_update_indexes(engine):
    drop_test_table(engine, 'idx1')
    drop_test_table(engine, 'idx2')
    create_test_table(engine, 'idx1')
    create_test_table(engine, 'idx2')
    edit_db.update_indexes(['idx1', 'idx2'], use_multiprocessing=False)
    # Check that indexes exist for both
    with engine.connect() as conn:
        idx1 = conn.execute(text("SELECT indexname FROM pg_indexes WHERE tablename = 'idx1';")).fetchall()
        idx2 = conn.execute(text("SELECT indexname FROM pg_indexes WHERE tablename = 'idx2';")).fetchall()
        assert any('idx_idx1_Kunta' in i[0] for i in idx1)
        assert any('idx_idx2_Kunta' in i[0] for i in idx2)
    drop_test_table(engine, 'idx1')
    drop_test_table(engine, 'idx2')

def test_remove_duplicates(engine):
    drop_test_table(engine, 'dup_table')
    with engine.connect() as conn:
        conn.execute(text('''
            CREATE TABLE "dup_table" (
                id SERIAL PRIMARY KEY,
                "Havainnon_tunniste" TEXT,
                "Lataus_pvm" TIMESTAMP,
                geometry Geometry(Point, 4326)
            );
        '''))
        # Insert duplicates
        conn.execute(text('''
            INSERT INTO "dup_table" ("Havainnon_tunniste", "Lataus_pvm", geometry) VALUES
            ('a', '2023-01-01', ST_GeomFromText('POINT(1 2)', 4326)),
            ('a', '2023-01-02', ST_GeomFromText('POINT(1 2)', 4326)),
            ('b', '2023-01-01', ST_GeomFromText('POINT(2 3)', 4326));
        '''))
        conn.commit()
    removed = edit_db.remove_duplicates(['dup_table'])
    # Only one duplicate should be removed
    assert removed == 1
    count = edit_db.get_amount_of_occurrences('dup_table')
    assert count == 2
    drop_test_table(engine, 'dup_table')
