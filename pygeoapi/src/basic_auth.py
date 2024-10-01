from src.app import auth, admin_api_auth
from src.models import APIKey, AdminAPIUser
from datetime import datetime
from werkzeug.security import generate_password_hash


@auth.verify_password
def verify_password(username, password):
    key_hash = generate_password_hash(username)

    api_key = APIKey.query.filter(APIKey.key_hash == key_hash).first()
    if api_key is not None and api_key.expire_date < datetime.now():
        return api_key


@admin_api_auth.verify_password
def verify_password(username, password):
    user = AdminAPIUser.query.filter(AdminAPIUser.system_id == username).first()

    if user is not None and user.verify_password(password):
        return user
