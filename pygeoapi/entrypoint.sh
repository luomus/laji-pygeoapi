#!/bin/bash

cp /pygeoapi/catalogue.tinydb /pygeoapi/metadata_db.tinydb

export PYGEOAPI_HOME=/pygeoapi
export PYGEOAPI_CONFIG="${PYGEOAPI_HOME}/local.config.yml"
export PYGEOAPI_OPENAPI="${PYGEOAPI_HOME}/local.openapi.yml"

# gunicorn env settings with defaults
SCRIPT_NAME=${SCRIPT_NAME:=/}
CONTAINER_NAME=${CONTAINER_NAME:=pygeoapi}
CONTAINER_HOST=${CONTAINER_HOST:=0.0.0.0}
CONTAINER_PORT=${CONTAINER_PORT:=80}
WSGI_WORKERS=${WSGI_WORKERS:=4}
WSGI_WORKER_TIMEOUT=${WSGI_WORKER_TIMEOUT:=6000}
WSGI_WORKER_CLASS=${WSGI_WORKER_CLASS:=gevent}

# generate openapi.yml
pygeoapi openapi generate ${PYGEOAPI_CONFIG} --output-file ${PYGEOAPI_OPENAPI}

# start the server
gunicorn --workers ${WSGI_WORKERS} \
				--worker-class=${WSGI_WORKER_CLASS} \
				--timeout ${WSGI_WORKER_TIMEOUT} \
				--name=${CONTAINER_NAME} \
				--bind ${CONTAINER_HOST}:${CONTAINER_PORT} \
				app:app