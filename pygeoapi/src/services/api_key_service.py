from src.app import db
from src.models import APIKey
from datetime import datetime, timedelta
import secrets


def get_api_keys(user_id=None):
    query = APIKey.query

    if user_id is not None:
        query = query.filter(
            APIKey.user_id == user_id
        )

    return query.all()


def generate_api_key(user_id, api_key_expires, data_use_purpose):
    key = secrets.token_urlsafe(48)
    expire_date = datetime.now() + timedelta(days=api_key_expires)

    api_key = APIKey(
        user_id=user_id,
        key=key,
        expires=expire_date,
        data_use_purpose=data_use_purpose
    )

    db.session.add(api_key)
    db.session.commit()

    return api_key


def api_key_object_to_string(api_key):
    return '{}-{}'.format(api_key.id, api_key.key)


def api_key_string_to_object(api_key_string):
    api_key_parts = api_key_string.split('-', 1)
    if len(api_key_parts) != 2:
        return

    try:
        api_key_id = int(api_key_parts[0])
    except ValueError:
        return

    api_key_value = api_key_parts[1]

    api_key = APIKey.query.filter(APIKey.id == api_key_id).first()

    if api_key is not None and api_key.verify_key(api_key_value):
        return api_key
