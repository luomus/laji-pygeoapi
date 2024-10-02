from src.app import db
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime


class APIKey(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.String(50), nullable=False)
    key_hash = db.Column(db.String(162), nullable=False)
    created = db.Column(db.DateTime(), nullable=False, default=datetime.now)
    expires = db.Column(db.DateTime(), nullable=False)
    data_use_purpose = db.Column(db.Text(), nullable=False)

    # Custom property getter
    @property
    def key(self):
        raise AttributeError('key is not a readable attribute')

    # Custom property setter
    @key.setter
    def key(self, key):
        self.key_hash = generate_password_hash(key)

    def verify_key(self, key):
        return check_password_hash(self.key_hash, key)


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
