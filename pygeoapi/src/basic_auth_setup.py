from src.app import app, auth
from src.services import request_log_service, laji_api
from datetime import datetime
from flask import request, g
import logging

logger = logging.getLogger(__name__)

@auth.verify_password
def verify_password(username, password):
    api_key = username.strip()
    if not api_key:
        return

    api_key_info = laji_api.get_api_key_info(api_key)
    
    if not api_key_info:
        logger.error('API key not found or invalid: %s', api_key)
        return None
    
    personId = api_key_info.get('personId')

    if (
        'found' in api_key_info and
        api_key_info['found'] and
        api_key_info['downloadType'] == app.config['API_KEY_TYPE'] and
        datetime.strptime(api_key_info['apiKeyExpires'], "%Y-%m-%d") > datetime.now()
    ):
        # Store personId in Flask's g object for this request
        g.personId = personId
        g.api_key_info = api_key_info
        return api_key_info['id']


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
