#!/bin/bash

. ./scripts/utils.sh

postgres_running_at_start = $(is_running postgres)

if [ -n "$postgres_running_at_start" ]; then
    docker compose up --build postgres &
fi

C='. /pygeoapi/setup.sh && flask --app src.app'
# parse command line arguments, escape double quotes
for i in "$@"; do
    i="${i//\\/\\\\}"
    C="$C \"${i//\"/\\\"}\""
done

echo "waiting for postgres..."
while ! is_healthy postgres; do sleep 1; done

docker compose run --rm --no-deps --build --entrypoint="/bin/bash -c '$C'" pygeoapi

if [ -n "$postgres_running_at_start" ]; then
    docker compose down
fi
