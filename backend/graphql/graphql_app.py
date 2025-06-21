from ariadne import make_executable_schema, load_schema_from_path
from backend.graphql.resolvers import (
    player_resolvers,
    team_resolvers,
    player_weekly_stats_resolvers,
    player_yearly_stats_resolvers,
    team_weekly_stats_resolvers,
    team_yearly_stats_resolvers
)

schema = make_executable_schema(
    load_schema_from_path("backend/graphql/schema.graphql"),
    player_resolvers.query,
    player_resolvers.mutation,
    team_resolvers.query,
    team_resolvers.mutation,
    player_weekly_stats_resolvers.query,
    player_weekly_stats_resolvers.mutation,
    player_yearly_stats_resolvers.query,
    player_yearly_stats_resolvers.mutation,
    team_weekly_stats_resolvers.query,
    team_weekly_stats_resolvers.mutation,
    team_yearly_stats_resolvers.query,
    team_yearly_stats_resolvers.mutation
)