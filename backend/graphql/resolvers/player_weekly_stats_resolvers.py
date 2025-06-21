from ariadne import QueryType, MutationType
from backend.models.player_weekly_stats import PlayerWeeklyStats

query = QueryType()
mutation = MutationType()

@query.field("allPlayerWeeklyStats")
def resolve_all(_, info):
    db = info.context["db"]
    return db.query(PlayerWeeklyStats).all()

@query.field("playerWeeklyStatsByPK")
def resolve_by_pk(_, info, player_id, season, season_type, week):
    db = info.context["db"]
    return db.query(PlayerWeeklyStats).filter_by(
        player_id=player_id, season=season, season_type=season_type, week=week
    ).first()

@mutation.field("addPlayerWeeklyStats")
def add(_, info, playerWeeklyStatsInput):
    db = info.context["db"]
    stat = PlayerWeeklyStats(**playerWeeklyStatsInput)
    db.add(stat)
    db.commit()
    db.refresh(stat)
    return stat

@mutation.field("updatePlayerWeeklyStats")
def update(_, info, player_id, season, season_type, week, playerWeeklyStatsInput):
    db = info.context["db"]
    stat = db.query(PlayerWeeklyStats).filter_by(
        player_id=player_id, season=season, season_type=season_type, week=week
    ).first()
    if stat:
        for key, value in playerWeeklyStatsInput.items():
            setattr(stat, key, value)
        db.commit()
        db.refresh(stat)
    return stat

@mutation.field("deletePlayerWeeklyStats")
def delete(_, info, player_id, season, season_type, week):
    db = info.context["db"]
    stat = db.query(PlayerWeeklyStats).filter_by(
        player_id=player_id, season=season, season_type=season_type, week=week
    ).first()
    if stat:
        db.delete(stat)
        db.commit()
        return True
    return False