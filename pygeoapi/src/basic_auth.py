from src.app import app, auth, admin_api_auth
from src.services import api_key_service, request_log_service
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
    if not _endpoint_requires_login(request.endpoint):
        return

    return login_required_dummy_view()


@app.after_request
def after_request(response):
    if not _endpoint_requires_login(request.endpoint):
        return response

    request_log_service.create_log_entry(request, response)

    return response


def _endpoint_requires_login(endpoint):
    if not endpoint:
        return False

    endpoint_root = endpoint.split('.', 1)[0]

    if endpoint in ['static', 'pygeoapi.landing_page'] or endpoint_root in ['admin_api']:
        return False

    return True
