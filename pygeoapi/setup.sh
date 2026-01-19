#!/bin/bash

# Activate virtual environment
source /venv/bin/activate

cp /pygeoapi/catalogue.tinydb /pygeoapi/metadata_db.tinydb

# generate openapi.yml
pygeoapi openapi generate ${PYGEOAPI_CONFIG} --output-file ${PYGEOAPI_OPENAPI}

# create datatable tables
flask --app src.app db upgrade