from tinydb import TinyDB
from datetime import datetime
from pathlib import Path
from datetime import datetime
from tinydb import TinyDB

def empty_metadata_db(metadata_db_path):
    """
    Empties the metadata database by dropping all tables.

    Parameters:
    metadata_db_path (str): The file path to the TinyDB database file.
    """
    db = TinyDB(metadata_db_path)
    db.drop_tables()
    db.close()

def create_metadata(metadata_dict, metadata_db_path):
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
    bbox = metadata_dict.get('bbox')
    minx, miny, maxx, maxy = bbox
    min_date = metadata_dict.get('min_date')
    min_day = min_date.split('T')[0]
    max_date = metadata_dict.get('max_date')
    max_day = max_date.split('T')[0]
    no_of_occurrences = metadata_dict.get('no_of_occurrences')
    table_no = metadata_dict.get('table_no')


    # Create a JSON record for the metadata to be inserted into the database.
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
            'type': 'surface',
            'title': dataset_name,
            'description': f'This dataset has {no_of_occurrences} occurrences from the area of {dataset_name}. The occurrences have been collected between {min_day} and {max_day}.',
            'providers': [],
            'externalIds': [{
                'scheme': 'default',
                'value': 'ID_'+str(table_no)
            }],
            'themes': 'themes',
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
            "href":"https://info.laji.fi/en/frontpage/services-and-instructions/spatial-data/spatial-data-services/ogc-api-in-qgis-python-or-r"
        },
        {
            "type":"text/html",
            "title":"General OGC API instructions",
            "href":"https://info.laji.fi/en/frontpage/services-and-instructions/spatial-data/spatial-data-services/ogc-api-instructions/"
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