#!/bin/bash

. setup.sh

CONTAINER_HOST=${CONTAINER_HOST:=0.0.0.0}
CONTAINER_PORT=${CONTAINER_PORT:=5000}

# start the development server
flask --app src.app --debug run --host=${CONTAINER_HOST} --port=${CONTAINER_PORT}
