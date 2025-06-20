import os
from flask import Flask, jsonify, request # Import request for handling POST data
from dotenv import load_dotenv

# Import Ariadne functions for schema and GraphQL execution
from ariadne import graphql_sync
from ariadne.explorer import ExplorerGraphiQL # For serving the GraphiQL interface

# Import the db instance from backend/__init__.py
from . import db

# Import the executable schema from backend/schema.py
# This 'schema' object is created in backend/schema.py by make_executable_schema
from .schema import schema

# --- Load environment variables from .env file ---
load_dotenv()

# Initialize the GraphiQL explorer HTML once
explorer_html = ExplorerGraphiQL().html(None)

def create_app():
    app = Flask(__name__)

    # --- Database Connection Configuration ---
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('DB_NAME')

    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
        raise ValueError("One or more database environment variables are not set. Check your .env file.")

    DATABASE_URL = (
        f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
    )

    app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    # Initialize SQLAlchemy with the Flask app
    db.init_app(app)

    # Test DB connection on startup (still crucial)
    with app.app_context():
        try:
            db.engine.connect()
            print(f"Successfully connected to MariaDB database '{DB_NAME}'!")
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            exit(1)

    # --- GraphQL Explorer (GET request) ---
    @app.route("/graphql", methods=["GET"])
    def graphql_explorer():
        # On GET request to /graphql, serve the GraphiQL explorer HTML.
        # This is what allows you to use the interactive GraphQL playground in your browser.
        return explorer_html, 200

    # --- GraphQL Server (POST request) ---
    @app.route("/graphql", methods=["POST"])
    def graphql_server():
        # GraphQL queries are always sent as POST requests with a JSON payload.
        data = request.get_json()

        # Execute the GraphQL query using the schema defined in backend/schema.py
        # context_value is where you can pass extra data (like the Flask request object,
        # or database session, or user info) to your GraphQL resolvers.
        success, result = graphql_sync(
            schema,
            data,
            context_value={"request": request}, # Pass Flask request to resolvers if they need it
            debug=app.debug # Use Flask's debug mode for Ariadne's debug output
        )

        status_code = 200 if success else 400
        return jsonify(result), status_code

    # --- Basic Health Check (still useful) ---
    @app.route('/health', methods=['GET'])
    def health_check():
        return jsonify({"status": "healthy", "message": "Backend GraphQL API is running!"}), 200

    return app

# --- Run the Flask app ---
# This block is executed when app.py is run directly (e.g., 'flask run')
if __name__ == '__main__':
    # Ensure FLASK_APP is set if running directly via python app.py
    # Though 'flask run' is the preferred way and handles this.
    # The FLASK_APP env var should point to 'backend.app:create_app()'
    app = create_app()
    app.run(debug=True, host='0.0.0.0', port=5000)