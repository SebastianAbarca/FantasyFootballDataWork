from ariadne import QueryType
from backend.db import SessionLocal
from backend.models import DimPlayers

query = QueryType()

@query.field("allPlayers")
def resolve_all_players(_, info):
    session = SessionLocal()
    players = session.query(DimPlayers).all()
    session.close()
    return players
