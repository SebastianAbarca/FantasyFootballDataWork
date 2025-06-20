from fastapi import FastAPI
from backend.graphql.graphql_app import graphql_app

app = FastAPI()

app.mount("/graphql", graphql_app)
