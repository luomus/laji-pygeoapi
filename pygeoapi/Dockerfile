FROM geopython/pygeoapi:latest

COPY pygeoapi/entrypoint.sh .

RUN chgrp -R 0 /pygeoapi && chmod -R g=u /pygeoapi
RUN chmod +x /pygeoapi/entrypoint.sh

ENTRYPOINT ["/pygeoapi/entrypoint.sh"]