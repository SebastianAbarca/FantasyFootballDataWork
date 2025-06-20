from ariadne import make_executable_schema, load_schema_from_path
from ariadne.asgi import GraphQL

from backend.graphql.resolvers.player_resolvers import player_query, player_mutation
from backend.graphql.resolvers.team_resolvers import team_query, team_mutation
from backend.graphql.resolvers.player_weekly_stats_resolvers import query as pws_query, mutation as pws_mutation

type_defs = load_schema_from_path("backend/graphql/schema.graphql")

schema = make_executable_schema(
    type_defs,
    [player_query, player_mutation, team_query, team_mutation, pws_query, pws_mutation]
)
graphql_app = GraphQL(schema, debug=True)