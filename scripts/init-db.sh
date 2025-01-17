#!/bin/bash

source ./scripts/utils.sh

postgres_running_at_start=$(is_running postgres; echo $?)

if [ "$postgres_running_at_start" -ne 0 ]; then
    echo "starting postgres"
    docker compose up --build postgres &
fi

echo "waiting for postgres..."
while ! is_healthy postgres; do sleep 1; done

echo "running scripts"
docker compose -f ./docker-compose-python-scripts.yaml up --build --force-recreate

if [ "$postgres_running_at_start" -ne 0 ]; then
    echo "cleaning up"
    docker compose down
fi
