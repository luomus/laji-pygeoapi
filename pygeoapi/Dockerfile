FROM geopython/pygeoapi:latest

RUN chgrp -R 0 /pygeoapi && chmod -R g=u /pygeoapi

EXPOSE 5000

ENTRYPOINT gunicorn --bind 0.0.0.0:5000 pygeoapi & pygeoapi serve