from flask import Flask, abort
from flask_httpauth import HTTPBasicAuth
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from src.config import Config
from pygeoapi.flask_app import BLUEPRINT as pygeoapi_blueprint

auth = HTTPBasicAuth()

app = Flask(__name__, static_folder='/pygeoapi/pygeoapi/static', static_url_path='/static')
app.config.from_object(Config)

db = SQLAlchemy(app)
migrate = Migrate(app, db, include_schemas=True)

'''
@auth.verify_password
def verify_password(username, password):
    return None

@app.before_request
@auth.login_required
def before_request():
    if not auth.current_user():
        return abort(401)
'''

app.register_blueprint(pygeoapi_blueprint, url_prefix='/')

import src.models