from ariadne import QueryType, MutationType
from backend.models.team_weekly_stats import TeamWeeklyStats

query = QueryType()
mutation = MutationType()

@query.field("allTeamWeeklyStats")
def resolve_all_team_weekly_stats(_, info):
    db = info.context["db"]
    return db.query(TeamWeeklyStats).all()

@query.field("teamWeeklyStatsByPK")
def resolve_team_weekly_stats_by_pk(_, info, game_id, team_id):
    db = info.context["db"]
    return db.query(TeamWeeklyStats).filter_by(game_id=game_id, team_id=team_id).first()

@mutation.field("addTeamWeeklyStats")
def resolve_add_team_weekly_stats(_, info, input):
    db = info.context["db"]
    stat = TeamWeeklyStats(**input)
    db.add(stat)
    db.commit()
    db.refresh(stat)
    return stat

@mutation.field("updateTeamWeeklyStats")
def resolve_update_team_weekly_stats(_, info, game_id, team_id, input):
    db = info.context["db"]
    stat = db.query(TeamWeeklyStats).filter_by(game_id=game_id, team_id=team_id).first()
    if stat:
        for key, value in input.items():
            setattr(stat, key, value)
        db.commit()
        db.refresh(stat)
    return stat

@mutation.field("deleteTeamWeeklyStats")
def resolve_delete_team_weekly_stats(_, info, game_id, team_id):
    db = info.context["db"]
    stat = db.query(TeamWeeklyStats).filter_by(game_id=game_id, team_id=team_id).first()
    if stat:
        db.delete(stat)
        db.commit()
        return True
    return False
