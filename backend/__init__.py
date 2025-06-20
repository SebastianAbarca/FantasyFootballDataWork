from flask import Flask
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()
db.app = Flask(__name__)