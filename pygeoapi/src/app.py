from flask import Flask, Response, request
from flask_httpauth import HTTPBasicAuth
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from src.config import Config

auth = HTTPBasicAuth()
admin_api_auth = HTTPBasicAuth()

app = Flask(__name__, static_folder='/pygeoapi/pygeoapi/static', static_url_path='/static')
app.config.from_object(Config)

db = SQLAlchemy(app)
migrate = Migrate(app, db, include_schemas=True)

import src.basic_auth


@app.before_request
def before_request():
    if request.endpoint in ['static', 'pygeoapi.landing_page', 'admin_api.post_api_key_request']:
        return

    if not auth.current_user():
        resp = Response('Unauthorized', 401)
        resp.headers['WWW-Authenticate'] = auth.authenticate_header()
        return resp


from pygeoapi.flask_app import BLUEPRINT as pygeoapi_blueprint
from src.views import admin_api_blueprint

app.register_blueprint(pygeoapi_blueprint, url_prefix='/')
app.register_blueprint(admin_api_blueprint, url_prefix='/admin/api')

from src.commands import *
