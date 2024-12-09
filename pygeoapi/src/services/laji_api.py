from src.app import app, cache
import requests


@cache.memoize(300)
def get_api_key_info(api_key):
    response = requests.get(
        '{}warehouse/api-keys/{}'.format(app.config['LAJI_API_URL'], api_key),
        { 'access_token': app.config['ACCESS_TOKEN'] }
    )

    data = response.json()
    return data
