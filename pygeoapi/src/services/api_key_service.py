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

    return api_key.id + '-' + key
