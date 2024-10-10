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
        return _error_response(400, 'Field "personToken" is missing')

    user_info = get_user_info(person_token)
    if user_info is None:
        return _error_response(400, 'Person token is invalid')

    if not any(role in app.config['API_KEY_ALLOWED_ROLES'] for role in user_info['roles']):
        return _error_response(403, 'Person is missing a required role')

    data_use_purpose = request.json.get('dataUsePurpose', None)
    if data_use_purpose is None:
        return _error_response(400, 'Field "dataUsePurpose" is missing')

    api_key_expires = request.json.get('apiKeyExpires', 90)
    if (
        not isinstance(api_key_expires, int) or
        api_key_expires < 1 or
        api_key_expires > app.config['API_KEY_MAX_DURATION']
    ):
        return _error_response(400, 'Field "apiKeyMissing" is missing or has an invalid value')

    user_id = user_info['qname']
    api_key = api_key_service.generate_api_key(user_id, api_key_expires, data_use_purpose)

    return _api_key_to_json(api_key, user_id)


@admin_api_blueprint.route('api-keys', methods=['GET'])
@admin_api_auth.login_required
def get_api_keys():
    person_token = request.args.get('personToken', None)

    user_id = None

    if person_token is not None:
        user_info = get_user_info(person_token)

        if user_info is None:
            return _error_response(400, 'Person token is invalid')

        user_id = user_info['qname']

    api_keys = api_key_service.get_api_keys(user_id)

    result = [_api_key_to_json(key, user_id) for key in api_keys]

    return result


def _api_key_to_json(api_key, user_id):
    result = {
        'personId': api_key.user_id,
        'requested': api_key.created.strftime('%Y-%m-%d'),
        'apiKeyExpires': api_key.expires.strftime('%Y-%m-%d'),
        'dataUsePurpose': api_key.data_use_purpose
    }

    if api_key.user_id == user_id:
        result['apiKey'] = api_key_service.api_key_object_to_string(api_key)

    return result


def _error_response(code, message):
    return {
        'code': code,
        'message': message
    }, code
