FROM geopython/pygeoapi:latest

RUN chgrp -R 0 /pygeoapi && chmod -R g=u /pygeoapi

ENTRYPOINT gunicorn pygeoapi.flask_app:APP