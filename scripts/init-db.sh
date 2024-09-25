#!/bin/bash

docker compose up --build -d postgres

echo "waiting for postgres..."

is_healthy() {
    service="$1"
    container_id="$(docker compose ps -q "$service")"
    health_status="$(docker inspect -f "{{.State.Health.Status}}" "$container_id")"

    if [ "$health_status" = "healthy" ]; then
        return 0
    else
        return 1
    fi
}

while ! is_healthy postgres; do sleep 1; done

echo "running scripts"

docker compose -f ./docker-compose-python-scripts.yaml up --build

echo "cleaning up"

docker compose down --remove-orphans
