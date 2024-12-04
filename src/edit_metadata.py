from tinydb import TinyDB
from datetime import datetime
from pathlib import Path
from datetime import datetime
from tinydb import TinyDB
import edit_db, compute_variables, edit_config
import os
from dotenv import load_dotenv


def empty_metadata_db(metadata_db_path):
    """
    Empties the metadata database by dropping all tables.

    Parameters:
    metadata_db_path (str): The file path to the TinyDB database file.
    """
    db = TinyDB(metadata_db_path)
    db.drop_tables()
    db.close()

def create_metadata(template_resource, metadata_db_path, pygeoapi_config_out):
    """
    Generates metadata for geospatial tables in the database and updates both 
    the PyGeoAPI configuration and a metadata database.
    
    Parameters:
    - template_resource (str): A template file for generating PyGeoAPI configuration with placeholders for dynamic values.
    - metadata_db_path (str): Path to the metadata database where JSON metadata for each table will be stored.
    - pygeoapi_config_out (str): Output path for the PyGeoAPI configuration file.
    """
    table_names = edit_db.get_all_tables()
    for idx, table_name in enumerate(table_names):
        bbox = edit_db.get_table_bbox(table_name)
        min_date, max_date = edit_db.get_table_dates(table_name)
        no_of_occurrences = edit_db.get_amount_of_occurrences(table_name)
        quality_dict = edit_db.get_quality_frequency(table_name)
        title_name = compute_variables.get_title_name_from_table_name(table_name)

        if no_of_occurrences == 0:
            continue

        if 'polygon' in table_name:
            geom_type = 'polygon'
        elif 'line' in table_name:
            geom_type = 'line'
        elif 'point' in table_name:
            geom_type = 'point'

        # Create parameters dictionary to fill the template for pygeoapi config file
        load_dotenv()
        template_params = {
            "<placeholder_table_name>": table_name,
            "<placeholder_geom_type>": geom_type,
            "<placeholder_title>": title_name,
            "<placeholder_amount_of_occurrences>": str(no_of_occurrences),
            "<placeholder_bbox>": str(bbox),
            "<placeholder_min_date>": min_date,
            "<placeholder_max_date>": max_date,
            "<placeholder_postgres_host>": os.getenv('POSTGRES_HOST'),
            "<placeholder_postgres_password>": os.getenv('POSTGRES_PASSWORD'),
            "<placeholder_postgres_user>": os.getenv('POSTGRES_USER'),
            "<placeholder_db_name>": os.getenv('POSTGRES_DB')
        }

        metadata_dict = {
            "bbox": bbox,
            "dataset_name": table_name,
            "geom_type": geom_type,
            "title_name": title_name,
            "no_of_occurrences": no_of_occurrences,
            "min_date": min_date,
            "max_date": max_date,
            "table_no": idx,
            "quality_dict": quality_dict
        }

        edit_config.add_to_pygeoapi_config(template_resource, template_params, pygeoapi_config_out)
        add_JSON_metadata_to_DB(metadata_dict, metadata_db_path)

def add_JSON_metadata_to_DB(metadata_dict, metadata_db_path):
    """
    Creates a JSON metadata record and inserts it into a TinyDB database.

    Parameters:
    metadata_dict (dict): A dictionary containing metadata information.
    metadata_db_path (str): The file path to the TinyDB database file.
    """
    
    db_path = Path(metadata_db_path)

    # Open the TinyDB database
    db = TinyDB(db_path)

    # Extract necessary fields from the metadata dictionary.
    dataset_name = metadata_dict.get('dataset_name')
    title_name = metadata_dict.get('title_name')
    geom_type = metadata_dict.get('geom_type')
    bbox = metadata_dict.get('bbox')
    minx, miny, maxx, maxy = bbox
    min_date = metadata_dict.get('min_date')
    min_day = min_date.split('T')[0]
    max_date = metadata_dict.get('max_date')
    max_day = max_date.split('T')[0]
    no_of_occurrences = metadata_dict.get('no_of_occurrences')
    table_no = metadata_dict.get('table_no')
    quality_dict = metadata_dict.get('quality_dict')
    professional = quality_dict.get('Ammattiaineistot / asiantuntijoiden laadunvarmistama')
    hobbyist = quality_dict.get('Asiantuntevat harrastajat / asiantuntijoiden laadunvarmistama')
    amateur = quality_dict.get('Kansalaishavaintoja / ei laadunvarmistusta')



    # Create a JSON metadata record to be inserted into the database.
    json_record = {
        'id': "ID_"+str(table_no),
        'conformsTo': [
            'http://www.opengis.net/spec/ogcapi-records-1/1.0/req/record-core'
        ],
        'type': 'Feature',
        'time': [min_date, max_date],
        'geometry': {
            'type': 'Polygon',
            'coordinates': [[
                [minx, miny],
                [minx, maxy],
                [maxx, maxy],
                [maxx, miny],
                [minx, miny]
            ]]
        },
        'properties': {
            'created': "2024-08-08",
            'updated': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'type': 'dataset',
            'title': f'{dataset_name}',
            'description': f'This dataset has {no_of_occurrences} {geom_type} occurrence features from the area of {title_name}. The data comes from multiple sources. Original data can be also found from laji.fi. The occurrences have been collected between {min_day} and {max_day}.',
            'providers': [{
                'name': 'Finnish Biodiversity Information Facility (FinBIF)',
                'roles': ['distributor', 'pointOfContact', 'publisher']
            }],
            'contactPoint': 'helpdesk@laji.fi',
            'externalIds': [{
                'scheme': 'default',
                'value': 'ID_'+str(table_no)
            }],
            'format': [
                {'name': 'GeoJSON', 'mediatype': 'geo+json'},
                {'name': 'HTML', 'mediatype':  'html'},
                {'name': 'CSV', 'mediatype':  'csv'}
            ],
            'themes': 'occurrences',
            'status': 'onGoing',
            'maintenanceFrequency': 'weekly',
            'extent': {
                "spatial": {
                    "bbox": [[minx, miny, maxx, maxy]],
                    "crs": "https://www.opengis.net/def/crs/EPSG/0/4326"
                },
                "temporal": {
                    "interval": [[min_date, max_date]],
                    "trs": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"
                }
            },
            'quality': f'The Finnish Biodiversity Information Facility (FinBIF) compiles datasets from many sources, including government, professional researchers and citizen scientists. Data accuracy varies significantly within and between datasets—and all data should not necessarily be used for all applications. In this collection, {professional} % of occurrences are collected by professionals, {hobbyist} % collected by non-professional specialists, and {amateur} % collected by wider community. Therefore quality varies a lot. See data columns and the links below',
            '_metadata-anytext': ''
        },
        'links': [
        {
            "href":f"https://geoapi.laji.fi/collections/{dataset_name}",
            "rel":"item",
            "title":"Dataset in LUOMUS OGC API Features service",
            "type":"OGCFeat"
        },
        {
            "type":"text/html",
            "title":"How to use data in QGIS",
            "href":"https://info.laji.fi/en/frontpage/services-and-instructions/spatial-data/spatial-data-services/ogc-api-in-qgis-python-or-r",
            "rel": "related"
        },
        {
            "type":"text/html",
            "title":"General OGC API instructions",
            "href":"https://info.laji.fi/en/frontpage/services-and-instructions/spatial-data/spatial-data-services/ogc-api-instructions/",
            "rel": "related"
        },
        {
            "type":"text/html",
            "title":"Source data metadata",
            "href":"https://laji.fi/theme/dataset-metadata",
            "rel": "related"
        },
        {
            "type":"text/html",
            "title":"About data quality",
            "href":"https://info.laji.fi/en/frontpage/data-management/data-quality/",
            "rel": "related"
        },
        ]
    }
    
    # Insert the JSON record into the TinyDB database
    try:
        res = db.insert(json_record)
        #print(f'Metadata record {xml_file} loaded with internal id {res}')
    except Exception as err:
        print(f'Error inserting record: {err}')

    # Close the database connection to ensure all changes are saved and resources are freed.
    db.close()