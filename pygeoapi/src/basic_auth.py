from src.app import auth, admin_api_auth
from src.models import APIKey, AdminAPIUser
from datetime import datetime


@auth.verify_password
def verify_password(username, password):
    api_key_parts = username.split('-', 1)
    if len(api_key_parts) != 2:
        return

    try:
        api_key_id = int(api_key_parts[0])
    except ValueError:
        return

    api_key_value = api_key_parts[1]

    api_key = APIKey.query.filter(APIKey.id == api_key_id).first()
    if api_key is not None and api_key.verify_key(api_key_value) and api_key.expires > datetime.now():
        return api_key


@admin_api_auth.verify_password
def verify_password(username, password):
    user = AdminAPIUser.query.filter(AdminAPIUser.system_id == username).first()

    if user is not None and user.verify_password(password):
        return user
