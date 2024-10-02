from flask import Blueprint, request, abort
from src.app import admin_api_auth
from src.app import app
from src.services.laji_auth import get_user_info
from src.services import api_key_service

admin_api_blueprint = Blueprint('admin_api', __name__)


@admin_api_blueprint.route('api-key-request', methods=['POST'])
@admin_api_auth.login_required
def post_api_key_request():
    person_token = request.json.get('personToken', None)
    if person_token is None:
        return abort(400)

    user_info = get_user_info(person_token)
    if user_info is None or not any(role in app.config['API_KEY_ALLOWED_ROLES'] for role in user_info['roles']):
        return abort(400)

    data_use_purpose = request.json.get('dataUsePurpose', None)
    if data_use_purpose is None:
        return abort(400)

    api_key_expires = request.json.get('apiKeyExpires', 90)
    if (
        not isinstance(api_key_expires, int) or
        api_key_expires < 1 or
        api_key_expires > app.config['API_KEY_MAX_DURATION']
    ):
        return abort(400)

    key = api_key_service.generate_api_key(user_info['qname'], api_key_expires, data_use_purpose)

    return {'apiKey': key}


@admin_api_blueprint.route('api-keys', methods=['GET'])
@admin_api_auth.login_required
def get_api_keys():
    person_token = request.args.get('personToken', None)

    user_id = None

    if person_token is not None:
        user_info = get_user_info(person_token)

        if user_info is None:
            return abort(400)

        user_id = user_info['qname']

    api_keys = api_key_service.get_api_keys(user_id)

    result = [{
        'userId': key.user_id,
        'created': key.created,
        'expires': key.expires,
        'dataUsePurpose': key.data_use_purpose
    } for key in api_keys]

    return result
