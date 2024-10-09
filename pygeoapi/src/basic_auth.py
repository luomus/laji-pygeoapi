from src.app import app, auth, admin_api_auth
from src.services import api_key_service
from src.models import AdminAPIUser
from datetime import datetime
from flask import request


@auth.verify_password
def verify_password(username, password):
    api_key = api_key_service.api_key_string_to_object(username)

    if api_key is not None and api_key.expires > datetime.now():
        return api_key


@admin_api_auth.verify_password
def verify_password(username, password):
    user = AdminAPIUser.query.filter(AdminAPIUser.system_id == username).first()

    if user is not None and user.verify_password(password):
        return user


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
