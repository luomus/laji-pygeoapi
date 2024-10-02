#!/bin/bash

. ./scripts/utils.sh

docker compose up --build --no-recreate postgres &

C='. /pygeoapi/setup.sh && flask --app src.app'
# parse command line arguments, escape double quotes
for i in "$@"; do
    i="${i//\\/\\\\}"
    C="$C \"${i//\"/\\\"}\""
done

echo "waiting for postgres..."

while ! is_healthy postgres; do sleep 1; done

docker compose run --rm --no-deps --build --entrypoint="/bin/bash -c '$C'" pygeoapi

docker compose down

