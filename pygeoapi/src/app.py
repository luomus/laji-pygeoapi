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


# a dummy callable to execute the login_required logic
login_required_dummy_view = auth.login_required(lambda: None)


@app.before_request
def before_request():
    if not request.endpoint:
        return

    endpoint_root = request.endpoint.split('.', 1)[0]

    if request.endpoint in ['static', 'pygeoapi.landing_page'] or endpoint_root in ['admin_api']:
        return

    return login_required_dummy_view()


from pygeoapi.flask_app import BLUEPRINT as pygeoapi_blueprint
from src.views import admin_api_blueprint

app.register_blueprint(pygeoapi_blueprint, url_prefix='/')
app.register_blueprint(admin_api_blueprint, url_prefix='/admin/api')

from src.commands import *
