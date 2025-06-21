from ariadne import QueryType, MutationType
from backend.models.dim_players import DimPlayers

query = QueryType()
mutation = MutationType()

# ---- Queries ----
@query.field("allPlayers")
def resolve_all_players(_, info):
    db = info.context["db"]
    return db.query(DimPlayers).all()

@query.field("playerById")
def resolve_player_by_id(_, info, player_id):
    db = info.context["db"]
    return db.query(DimPlayers).filter_by(player_id=player_id).first()

# ---- Mutations ----
@mutation.field("addPlayer")
def resolve_add_player(_, info, **kwargs):
    db = info.context["db"]
    new_player = DimPlayers(**kwargs)
    db.add(new_player)
    db.commit()
    db.refresh(new_player)
    return new_player

@mutation.field("updatePlayer")
def resolve_update_player(_, info, player_id, **kwargs):
    db = info.context["db"]
    player = db.query(DimPlayers).filter_by(player_id=player_id).first()
    if not player:
        return None
    for key, value in kwargs.items():
        if value is not None:
            setattr(player, key, value)
    db.commit()
    db.refresh(player)
    return player

@mutation.field("deletePlayer")
def resolve_delete_player(_, info, player_id):
    db = info.context["db"]
    player = db.query(DimPlayers).filter_by(player_id=player_id).first()
    if not player:
        return False
    db.delete(player)
    db.commit()
    return True
