#!/bin/bash

. ./scripts/utils.sh

docker compose up --build --no-recreate postgres &

echo "waiting for postgres..."

while ! is_healthy postgres; do sleep 1; done

echo "running scripts"

docker compose -f ./docker-compose-python-scripts.yaml up --build

echo "cleaning up"

docker compose down
