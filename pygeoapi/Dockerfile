FROM geopython/pygeoapi:latest

RUN apt-get update && \
    apt-get install -y dos2unix

COPY pygeoapi/entrypoint.sh .
COPY pygeoapi/requirements-custom.txt .
COPY pygeoapi/custom/templates ./custom/templates/
COPY pygeoapi/custom/static ./pygeoapi/static/
COPY pygeoapi/app.py .

RUN python3 -m pip install --no-cache-dir -r requirements-custom.txt

RUN chgrp -R 0 /pygeoapi && chmod -R g=u /pygeoapi
RUN chmod +x /pygeoapi/entrypoint.sh

RUN dos2unix /pygeoapi/entrypoint.sh

ENTRYPOINT ["/pygeoapi/entrypoint.sh"]