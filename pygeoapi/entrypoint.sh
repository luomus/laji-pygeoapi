#!/bin/bash

cp /pygeoapi/catalogue.tinydb /pygeoapi/metadata_db.tinydb
chmod 777 /pygeoapi/metadata_db.tinydb

/entrypoint.sh