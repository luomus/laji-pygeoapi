FROM geopython/pygeoapi:latest

COPY custom_templates .

RUN chgrp -R 0 /pygeoapi && chmod -R g=u /pygeoapi
RUN chgrp 0 /pygeoapi/catalogue.tinydb && chmod g=u /pygeoapi/catalogue.tinydb