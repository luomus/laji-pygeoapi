is_healthy() {
    service="$1"
    container_id="$(docker compose ps -q "$service")"

    if [ -z "$container_id" ]; then
      return 1
    fi

    health_status="$(docker inspect -f "{{.State.Health.Status}}" "$container_id")"

    if [ "$health_status" = "healthy" ]; then
        return 0
    else
        return 1
    fi
}
