from ariadne import QueryType, MutationType
from backend.db import SessionLocal
from backend.models.dim_players import DimPlayers

player_query = QueryType()
player_mutation = MutationType()

# ---- Queries ----
@player_query.field("allPlayers")
def resolve_all_players(_, info):
    with SessionLocal() as session:
        return session.query(DimPlayers).all()

@player_query.field("playerById")
def resolve_player_by_id(_, info, player_id):
    with SessionLocal() as session:
        return session.query(DimPlayers).filter_by(player_id=player_id).first()

# ---- Mutations ----
@player_mutation.field("addPlayer")
def resolve_add_player(_, info, **kwargs):
    with SessionLocal() as session:
        new_player = DimPlayers(**kwargs)
        session.add(new_player)
        session.commit()
        session.refresh(new_player)
        return new_player

@player_mutation.field("updatePlayer")
def resolve_update_player(_, info, player_id, **kwargs):
    with SessionLocal() as session:
        player = session.query(DimPlayers).filter_by(player_id=player_id).first()
        if not player:
            return None
        for key, value in kwargs.items():
            if value is not None:
                setattr(player, key, value)
        session.commit()
        session.refresh(player)
        return player

@player_mutation.field("deletePlayer")
def resolve_delete_player(_, info, player_id):
    with SessionLocal() as session:
        player = session.query(DimPlayers).filter_by(player_id=player_id).first()
        if not player:
            return False
        session.delete(player)
        session.commit()
        return True
