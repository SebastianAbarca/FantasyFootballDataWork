from ariadne import QueryType, MutationType
from backend.models.dim_teams import DimTeams

query = QueryType()
mutation = MutationType()

@query.field("allTeams")
def resolve_all_teams(_, info):
    db = info.context["db"]
    return db.query(DimTeams).all()

@query.field("teamById")
def resolve_team_by_id(_, info, team_id):
    db = info.context["db"]
    return db.query(DimTeams).filter_by(team_id=team_id).first()

@mutation.field("addTeam")
def resolve_add_team(_, info, team_id):
    db = info.context["db"]
    team = DimTeams(team_id=team_id)
    db.add(team)
    db.commit()
    db.refresh(team)
    return team

@mutation.field("updateTeam")
def resolve_update_team(_, info, team_id):
    db = info.context["db"]
    team = db.query(DimTeams).filter_by(team_id=team_id).first()
    if not team:
        return None
    # No fields to update for now
    return team

@mutation.field("deleteTeam")
def resolve_delete_team(_, info, team_id):
    db = info.context["db"]
    team = db.query(DimTeams).filter_by(team_id=team_id).first()
    if not team:
        return False
    db.delete(team)
    db.commit()
    return True
