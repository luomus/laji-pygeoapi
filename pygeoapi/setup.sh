#!/bin/bash

cp /pygeoapi/catalogue.tinydb /pygeoapi/metadata_db.tinydb

# generate openapi.yml
pygeoapi openapi generate ${PYGEOAPI_CONFIG} --output-file ${PYGEOAPI_OPENAPI}
