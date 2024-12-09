from flask import Flask
from flask_httpauth import HTTPBasicAuth
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_caching import Cache
from src.config import Config

app = Flask(__name__, static_folder='/pygeoapi/pygeoapi/static', static_url_path='/static')
app.config.from_object(Config)

if app.config['RESTRICT_ACCESS']:
    auth = HTTPBasicAuth()

    db = SQLAlchemy(app)
    migrate = Migrate(app, db, include_schemas=True)
    cache = Cache(app)

    import src.basic_auth_setup # noqa

from pygeoapi.flask_app import BLUEPRINT as pygeoapi_blueprint # noqa

app.register_blueprint(pygeoapi_blueprint, url_prefix='/')

from src.commands import * # noqa
