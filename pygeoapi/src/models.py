from src.app import db
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime


class APIKey(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.String(50), nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)
    created_date = db.Column(db.DateTime(), nullable=False, default=datetime.now)
    expire_date = db.Column(db.DateTime(), nullable=False)

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
