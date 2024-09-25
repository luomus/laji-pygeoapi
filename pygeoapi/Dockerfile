FROM geopython/pygeoapi:latest

COPY pygeoapi/requirements-custom.txt .
RUN python3 -m pip install --no-cache-dir -r requirements-custom.txt

COPY pygeoapi/setup.sh .
COPY pygeoapi/entrypoint.sh .
COPY pygeoapi/custom/templates ./custom/templates/
COPY pygeoapi/custom/static ./pygeoapi/static/
COPY pygeoapi/src ./src

RUN chgrp -R 0 /pygeoapi && chmod -R g=u /pygeoapi
RUN chmod +x /pygeoapi/entrypoint.sh

ENTRYPOINT ["/pygeoapi/entrypoint.sh"]
