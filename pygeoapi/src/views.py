from flask import Blueprint, request
from src.app import admin_api_auth

admin_api_blueprint = Blueprint('admin_api', __name__)


@admin_api_blueprint.route('apiKeyRequest', methods=['POST'])
@admin_api_auth.login_required
def post_api_key_request():
    # personToken string
    # dataUsePurpose string
    # apiKeyExpires number

    return {'success': True}
