from backend import db

class DimTeams(db.Model):
    __tablename__ = 'DimTeams'
    team_id = db.Column(db.String(10), primary_key=True)
    team_name = db.Column(db.String(255))
    city = db.Column(db.String(255))
    state = db.Column(db.String(50))
    conference = db.Column(db.String(50))
    division = db.Column(db.String(50))