from ariadne import QueryType, MutationType
from backend.models.player_yearly_stats import PlayerYearlyStats

query = QueryType()
mutation = MutationType()

@query.field("allPlayerYearlyStats")
def resolve_all_player_yearly_stats(_, info):
    db = info.context["db"]
    return db.query(PlayerYearlyStats).all()

@query.field("playerYearlyStatsByPK")
def resolve_player_yearly_stats_by_pk(_, info, player_id, season, season_type, week):
    db = info.context["db"]
    return (
        db.query(PlayerYearlyStats)
        .filter_by(player_id=player_id, season=season, season_type=season_type, week=week)
        .first()
    )

@mutation.field("addPlayerYearlyStats")
def resolve_add_player_yearly_stats(_, info, playerYearlyStatsInput):
    db = info.context["db"]
    new_record = PlayerYearlyStats(**playerYearlyStatsInput)
    db.add(new_record)
    db.commit()
    db.refresh(new_record)
    return new_record

@mutation.field("updatePlayerYearlyStats")
def resolve_update_player_yearly_stats(_, info, player_id, season, season_type, week, playerYearlyStatsInput):
    db = info.context["db"]
    record = (
        db.query(PlayerYearlyStats)
        .filter_by(player_id=player_id, season=season, season_type=season_type, week=week)
        .first()
    )
    if not record:
        return None
    for key, value in playerYearlyStatsInput.items():
        if value is not None:
            setattr(record, key, value)
    db.commit()
    db.refresh(record)
    return record

@mutation.field("deletePlayerYearlyStats")
def resolve_delete_player_yearly_stats(_, info, player_id, season, season_type, week):
    db = info.context["db"]
    record = (
        db.query(PlayerYearlyStats)
        .filter_by(player_id=player_id, season=season, season_type=season_type, week=week)
        .first()
    )
    if not record:
        return False
    db.delete(record)
    db.commit()
    return True
