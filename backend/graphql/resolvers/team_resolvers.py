# backend/graphql/team_resolvers.py
from ariadne import QueryType, MutationType
from backend.db import SessionLocal
from backend.models.dim_teams import DimTeams

team_query = QueryType()
team_mutation = MutationType()

@team_query.field("allTeams")
def resolve_all_teams(_, info):
    with SessionLocal() as session:
        return session.query(DimTeams).all()

@team_query.field("teamById")
def resolve_team_by_id(_, info, team_id):
    with SessionLocal() as session:
        return session.query(DimTeams).filter_by(team_id=team_id).first()

@team_mutation.field("addTeam")
def resolve_add_team(_, info, team_id):
    with SessionLocal() as session:
        team = DimTeams(team_id=team_id)
        session.add(team)
        session.commit()
        session.refresh(team)
        return team

@team_mutation.field("updateTeam")
def resolve_update_team(_, info, team_id):
    with SessionLocal() as session:
        team = session.query(DimTeams).filter_by(team_id=team_id).first()
        if not team:
            return None
        # for now no fields to update
        return team

@team_mutation.field("deleteTeam")
def resolve_delete_team(_, info, team_id):
    with SessionLocal() as session:
        team = session.query(DimTeams).filter_by(team_id=team_id).first()
        if not team:
            return False
        session.delete(team)
        session.commit()
        return True
