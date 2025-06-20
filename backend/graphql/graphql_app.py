from ariadne import make_executable_schema, load_schema_from_path
from ariadne.asgi import GraphQL
from backend.graphql.resolvers import query

type_defs = load_schema_from_path("backend/graphql/schema.graphql")
schema = make_executable_schema(type_defs, query)

graphql_app = GraphQL(schema, debug=True)
