ARG target

FROM geopython/pygeoapi:latest AS base

ENV PYGEOAPI_HOME=/pygeoapi
ENV PYGEOAPI_CONFIG="${PYGEOAPI_HOME}/local.config.yml"
ENV PYGEOAPI_OPENAPI="${PYGEOAPI_HOME}/local.openapi.yml"

COPY pygeoapi/requirements-custom.txt .
RUN python3 -m pip install --no-cache-dir -r requirements-custom.txt

COPY pygeoapi/setup.sh .
COPY pygeoapi/entrypoint.sh .
COPY pygeoapi/entrypoint-dev.sh .
COPY pygeoapi/migrations ./migrations
COPY pygeoapi/src ./src

RUN chgrp -R 0 /pygeoapi && chmod -R g=u /pygeoapi
RUN chmod +x /pygeoapi/entrypoint.sh && chmod +x /pygeoapi/entrypoint-dev.sh

ENTRYPOINT ["/pygeoapi/entrypoint.sh"]

FROM base AS laji-pygeoapi-default

ENV RESTRICT_ACCESS="False"

COPY pygeoapi/custom/default-templates ./custom/templates/
COPY pygeoapi/custom/default-static ./pygeoapi/static/

FROM base AS laji-pygeoapi-virva

ENV RESTRICT_ACCESS="True"

COPY pygeoapi/custom/virva-templates ./custom/templates/
COPY pygeoapi/custom/virva-static ./pygeoapi/static/

FROM laji-pygeoapi-${target} AS final
