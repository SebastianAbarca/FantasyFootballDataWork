from backend import db

class DimPlayers(db.Model):
    __tablename__ = 'DimPlayers'
    player_id = db.Column(db.String(50), primary_key=True)
    player_name = db.Column(db.String(255))
    position = db.Column(db.String(10))
    birth_year = db.Column(db.Integer)
    draft_year = db.Column(db.Integer)
    draft_round = db.Column(db.Integer)
    draft_pick = db.Column(db.Integer)
    draft_ovr = db.Column(db.Integer)
    height = db.Column(db.Integer)
    weight = db.Column(db.Integer)
    college = db.Column(db.String(255))
