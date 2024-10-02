from src.app import app
import requests


def get_user_info(token):
    response = requests.get('{}token/{}'.format(app.config['LAJI_AUTH_URL'], token))

    data = response.json()

    if 'user' in data:
        return data['user']
