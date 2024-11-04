import os

from src.app import db
from werkzeug.security import generate_password_hash, check_password_hash
from cryptography.fernet import Fernet
from datetime import datetime


class APIKey(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.String(50), nullable=False)
    key_encrypted = db.Column(db.String(200), nullable=False)
    created = db.Column(db.DateTime(), nullable=False, default=datetime.now)
    expires = db.Column(db.DateTime(), nullable=False)
    data_use_purpose = db.Column(db.Text(), nullable=False)

    # Custom property getter
    @property
    def key(self):
        return APIKey.decrypt_key(self.key_encrypted)

    # Custom property setter
    @key.setter
    def key(self, key):
        self.key_encrypted = APIKey.encrypt_key(key)

    def verify_key(self, key):
        return self.key == key

    @staticmethod
    def encrypt_key(key):
        return Fernet(os.environ['SECRET_KEY']).encrypt(key.encode()).decode()

    @staticmethod
    def decrypt_key(key):
        return Fernet(os.environ['SECRET_KEY']).decrypt(key).decode()


class AdminAPIUser(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    system_id = db.Column(db.String(50), nullable=False)
    password_hash = db.Column(db.String(162), nullable=False)

    # Custom property getter
    @property
    def password(self):
        raise AttributeError('password is not a readable attribute')

    # Custom property setter
    @password.setter
    def password(self, password):
        self.password_hash = generate_password_hash(password)

    def verify_password(self, password):
        return check_password_hash(self.password_hash, password)


class RequestLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    status_code = db.Column(db.Integer, nullable=False)
    date = db.Column(db.DateTime(), nullable=False, default=datetime.now)
    api_key_id = db.Column(db.Integer)
    path = db.Column(db.Text(), nullable=False)
    query_string = db.Column(db.Text(), nullable=False)
    ip_address = db.Column(db.String(39), nullable=False)
