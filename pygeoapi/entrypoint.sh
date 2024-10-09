#!/bin/bash

. setup.sh

# gunicorn env settings with defaults
SCRIPT_NAME=${SCRIPT_NAME:=/}
CONTAINER_NAME=${CONTAINER_NAME:=pygeoapi}
CONTAINER_HOST=${CONTAINER_HOST:=0.0.0.0}
CONTAINER_PORT=${CONTAINER_PORT:=5000}
WSGI_WORKERS=${WSGI_WORKERS:=4}
WSGI_WORKER_TIMEOUT=${WSGI_WORKER_TIMEOUT:=6000}
WSGI_WORKER_CLASS=${WSGI_WORKER_CLASS:=gevent}

# start the server
gunicorn --workers ${WSGI_WORKERS} \
				--worker-class=${WSGI_WORKER_CLASS} \
				--timeout ${WSGI_WORKER_TIMEOUT} \
				--name=${CONTAINER_NAME} \
				--bind ${CONTAINER_HOST}:${CONTAINER_PORT} \
				src.app:app