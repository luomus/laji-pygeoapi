FROM geopython/pygeoapi:latest

COPY pygeoapi/entrypoint.sh .

RUN chgrp -R 0 /pygeoapi && chmod -R g=u /pygeoapi
RUN chgrp 0 /pygeoapi/entrypoint.sh && chmod g=u /pygeoapi/entrypoint.sh && chmod +x /pygeoapi/entrypoint.sh

ENTRYPOINT ["/pygeoapi/entrypoint.sh"]