import pandas as pd
from sqlalchemy import create_engine, text
import pymysql as mysql
import os
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

# Ensure all critical variables are loaded
if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
    print("Error: One or more database environment variables are not set. Check your .env file.")
    print("DB_USER:", DB_USER)
    print("DB_PASSWORD:", DB_PASSWORD)
    print("DB_HOST:", DB_HOST)
    print("DB_NAME:", DB_NAME)
    import sys
    sys.exit(1)

DATABASE_URL = (
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
)


try:
    engine = create_engine(DATABASE_URL)

    # Test the connection (optional, but good for verification)
    with engine.connect() as connection:
        # A simple query to check if the connection is active
        result = connection.execute(text("SELECT 1")).scalar()
        if result == 1:
            print(f"Successfully connected to MariaDB database '{DB_NAME}'!")
        else:
            print("Connection test returned unexpected result.")

except Exception as e:
    print(f"Error connecting to the database: {e}")
    # It's good practice to exit if the DB connection fails
    import sys
    sys.exit(1)

print("\nDatabase connection setup complete. You can now use the 'engine' object for ETL operations.")

cwd = os.getcwd()
parent_dir = os.path.dirname(cwd)
sub_directory = "myData"
file = 'my_player_weekly_stats_offense.csv'
target_folder = os.path.join(parent_dir, sub_directory)
target_file = os.path.join(target_folder, file)

print("Starting ETL process...")
print("Starting with DimTeams...")
try:
    print("target_file: ", target_file)
    df_temp = pd.read_csv(target_file)
    df_dim_teams = df_temp[['team']].drop_duplicates().rename(columns={'team': 'team_id'})

    # Handle potential nulls in 'team' column if they could exist in source
    df_dim_teams.dropna(subset=['team_id'], inplace=True)
    #print(df_dim_teams.shape)
    #for index, row in df_dim_teams.iterrows():  # Iterates through (index, Series) pairs
    #    print(row['team_id'])


    df_dim_teams.to_sql('DimTeams', con=engine, if_exists='append', index=False)
    print("DimTeams loaded successfully.")

except FileNotFoundError:
    print("Error: 'my_player_weekly_stats_offense.csv' not found. Please ensure it's in the correct directory.")
except Exception as e:
    print(f"An error occurred during DimTeams ETL: {e}")

