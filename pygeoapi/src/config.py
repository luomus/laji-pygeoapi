import os


class Config(object):
    DEBUG = False
    TESTING = False

    RESTRICT_ACCESS = os.environ['RESTRICT_ACCESS'] == 'True'

    SQLALCHEMY_DATABASE_URI = 'postgresql://{}:{}@{}:{}/{}'.format(
        os.environ['POSTGRES_USER'],
        os.environ['POSTGRES_PASSWORD'],
        os.environ['POSTGRES_HOST'],
        5432,
        os.environ['POSTGRES_DB']
    )

    LAJI_AUTH_URL = os.environ['LAJI_AUTH_URL']
    LAJI_API_URL = os.environ['LAJI_API_URL']
    ACCESS_TOKEN = os.environ['ACCESS_TOKEN']

    CACHE_TYPE = 'SimpleCache'
    CACHE_DEFAULT_TIMEOUT = 300

    API_KEY_TYPE = 'AUTHORITIES_VIRVA_GEOAPI_KEY'