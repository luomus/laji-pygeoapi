from src.app import app, cache
import requests


@cache.memoize(1200) # cache 1200 seconds = 20 minutes
def get_api_key_info(api_key):
    url = '{}warehouse/api-keys/{}'.format(app.config['LAJI_API_URL'], api_key)
    params = { 'access_token': app.config['ACCESS_TOKEN'] }
    for _ in range(3): 
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            continue
    return None
