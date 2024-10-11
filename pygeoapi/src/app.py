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


if app.config['SENSITIVE_DATA']:
    import src.basic_auth # noqa
    from src.views import admin_api_blueprint

    app.register_blueprint(admin_api_blueprint, url_prefix='/admin/api')


from pygeoapi.flask_app import BLUEPRINT as pygeoapi_blueprint # noqa

app.register_blueprint(pygeoapi_blueprint, url_prefix='/')

from src.commands import * # noqa
