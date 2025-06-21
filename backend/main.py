from fastapi import FastAPI, Request
from ariadne.asgi import GraphQL
from backend.graphql.graphql_app import schema
from backend.db import get_db

app = FastAPI()

@app.middleware("http")
async def db_session_middleware(request: Request, call_next):
    request.state.db = next(get_db())
    response = await call_next(request)
    return response

def get_context_value(request: Request):
    return {
        "request": request,
        "db": request.state.db
    }

app.add_route("/graphql", GraphQL(schema, context_value=get_context_value))