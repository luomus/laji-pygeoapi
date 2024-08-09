FROM geopython/pygeoapi:latest

COPY pygeoapi/entrypoint.sh .

RUN chgrp -R 0 /pygeoapi && chmod -R g=u /pygeoapi

ENTRYPOINT ["/pygeoapi/entrypoint.sh"]