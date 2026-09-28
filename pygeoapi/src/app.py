import logging

from flask import Flask
from flask_httpauth import HTTPBasicAuth
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_caching import Cache
from src.config import Config

# pygeoapi's l10n.str2locale() warns on every non-locale dict key (e.g. host,
# port, begin, end) when rendering HTML templates, regardless of the
# configured logging level, so silence it explicitly here.
logging.getLogger('pygeoapi.l10n').setLevel(logging.ERROR)

app = Flask(__name__, static_folder='/pygeoapi/pygeoapi/static', static_url_path='/static')
app.config.from_object(Config)

auth = HTTPBasicAuth()

db = SQLAlchemy(app)
migrate = Migrate(app, db, include_schemas=True)
cache = Cache(app)

if app.config['RESTRICT_ACCESS']:
    import src.basic_auth_setup # noqa

from pygeoapi.flask_app import BLUEPRINT as pygeoapi_blueprint # noqa

app.register_blueprint(pygeoapi_blueprint, url_prefix='/')

from src.commands import * # noqa
