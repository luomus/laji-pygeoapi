from src.app import db
from datetime import datetime

class RequestLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    status_code = db.Column(db.Integer, nullable=False)
    date = db.Column(db.DateTime(), nullable=False, default=datetime.now)
    api_key_id = db.Column(db.String(15), nullable=True)
    path = db.Column(db.Text(), nullable=False)
    query_string = db.Column(db.Text(), nullable=False)
    ip_address = db.Column(db.String(39), nullable=False)
