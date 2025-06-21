from ariadne import QueryType, MutationType
from backend.models.team_yearly_stats import TeamYearlyStats

query = QueryType()
mutation = MutationType()

# ----------- Query Resolvers -----------

@query.field("allTeamYearlyStats")
def resolve_all_team_yearly_stats(_, info):
    db = info.context["db"]
    return db.query(TeamYearlyStats).all()

@query.field("teamYearlyStatsByPK")
def resolve_team_yearly_stats_by_pk(_, info, team_id, season, season_type):
    db = info.context["db"]
    return db.query(TeamYearlyStats).filter_by(
        team_id=team_id,
        season=season,
        season_type=season_type
    ).first()

# ----------- Mutation Resolvers -----------

@mutation.field("addTeamYearlyStats")
def resolve_add_team_yearly_stats(_, info, input):
    db = info.context["db"]
    record = TeamYearlyStats(**input)
    db.add(record)
    db.commit()
    db.refresh(record)
    return record

@mutation.field("updateTeamYearlyStats")
def resolve_update_team_yearly_stats(_, info, team_id, season, season_type, input):
    db = info.context["db"]
    record = db.query(TeamYearlyStats).filter_by(
        team_id=team_id,
        season=season,
        season_type=season_type
    ).first()
    if not record:
        return None
    for key, value in input.items():
        if value is not None:
            setattr(record, key, value)
    db.commit()
    db.refresh(record)
    return record

@mutation.field("deleteTeamYearlyStats")
def resolve_delete_team_yearly_stats(_, info, team_id, season, season_type):
    db = info.context["db"]
    record = db.query(TeamYearlyStats).filter_by(
        team_id=team_id,
        season=season,
        season_type=season_type
    ).first()
    if not record:
        return False
    db.delete(record)
    db.commit()
    return True
