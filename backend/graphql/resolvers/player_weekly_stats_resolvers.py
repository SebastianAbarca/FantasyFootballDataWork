from ariadne import QueryType, MutationType
from backend.db import SessionLocal
from backend.models.player_weekly_stats import PlayerWeeklyStats

query = QueryType()
mutation = MutationType()

@query.field("allPlayerWeeklyStats")
def resolve_all_player_weekly_stats(_, info):
    with SessionLocal() as session:
        return session.query(PlayerWeeklyStats).all()

@query.field("playerWeeklyStatsByPK")
def resolve_player_weekly_stats_by_pk(_, info, player_id, season, season_type, week):
    with SessionLocal() as session:
        return (
            session.query(PlayerWeeklyStats)
            .filter_by(player_id=player_id, season=season, season_type=season_type, week=week)
            .first()
        )

@mutation.field("addPlayerWeeklyStats")
def resolve_add_player_weekly_stats(_, info, playerWeeklyStatsInput):
    with SessionLocal() as session:
        new_record = PlayerWeeklyStats(**playerWeeklyStatsInput)
        session.add(new_record)
        session.commit()
        session.refresh(new_record)
        return new_record

@mutation.field("updatePlayerWeeklyStats")
def resolve_update_player_weekly_stats(_, info, player_id, season, season_type, week, playerWeeklyStatsInput):
    with SessionLocal() as session:
        record = (
            session.query(PlayerWeeklyStats)
            .filter_by(player_id=player_id, season=season, season_type=season_type, week=week)
            .first()
        )
        if not record:
            return None
        for key, value in playerWeeklyStatsInput.items():
            if value is not None:
                setattr(record, key, value)
        session.commit()
        session.refresh(record)
        return record

@mutation.field("deletePlayerWeeklyStats")
def resolve_delete_player_weekly_stats(_, info, player_id, season, season_type, week):
    with SessionLocal() as session:
        record = (
            session.query(PlayerWeeklyStats)
            .filter_by(player_id=player_id, season=season, season_type=season_type, week=week)
            .first()
        )
        if not record:
            return False
        session.delete(record)
        session.commit()
        return True
