#!/bin/bash

. ./scripts/utils.sh

postgres_running_at_start = $(is_running postgres)

if [ -n "$postgres_running_at_start" ]; then
    docker compose up --build postgres &
fi

echo "waiting for postgres..."
while ! is_healthy postgres; do sleep 1; done

echo "running scripts"
docker compose -f ./docker-compose-python-scripts.yaml up --build

if [ -n "$postgres_running_at_start" ]; then
    echo "cleaning up"
    docker compose down
fi
