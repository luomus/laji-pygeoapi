import os


class Config(object):
    DEBUG = False
    TESTING = False

    SQLALCHEMY_DATABASE_URI = 'postgresql://{}:{}@{}:{}/{}'.format(
        os.environ['POSTGRES_USER'],
        os.environ['POSTGRES_PASSWORD'],
        os.environ['POSTGRES_HOST'],
        5432,
        os.environ['POSTGRES_DB']
    )

    LAJI_AUTH_URL = os.environ['LAJI_AUTH_URL']

    API_KEY_MAX_DURATION = 365
    API_KEY_ALLOWED_ROLES = ['MA.admin', 'MA.securePortalUser']

