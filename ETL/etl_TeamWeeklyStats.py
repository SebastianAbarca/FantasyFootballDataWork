import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import pymysql
from dotenv import load_dotenv

# --- Load environment variables from .env file ---
load_dotenv()

# --- Database Connection Configuration ---
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
    with engine.connect() as connection:
        result = connection.execute(text("SELECT 1")).scalar()
        if result == 1:
            print(f"Successfully connected to MariaDB database '{DB_NAME}'!")
        else:
            print("Connection test returned unexpected result.")
except Exception as e:
    print(f"Error connecting to the database: {e}")
    import sys

    sys.exit(1)

# print("\nDatabase connection setup complete. You can now use the 'engine' object for ETL operations.")

# --- File Path Configuration ---
cwd = os.getcwd()
parent_dir = os.path.dirname(cwd)
sub_directory = "myData"
file_weekly_offense = 'my_team_weekly_stats_offense.csv'
file_weekly_defense = 'my_team_weekly_stats_defense.csv'

target_folder = os.path.join(parent_dir, sub_directory)
target_file_weekly_offense = os.path.join(target_folder, file_weekly_offense)
target_file_weekly_defense = os.path.join(target_folder, file_weekly_defense)

# --- ETL for PlayerWeeklyStats ---
print("\nLoading PlayerWeeklyStats...")
try:
    df_weekly_off_raw = pd.read_csv(target_file_weekly_offense)
    df_weekly_def_raw = pd.read_csv(target_file_weekly_defense)
    print("\nOriginal Offense shape: ", df_weekly_off_raw.shape)
    print("\nOriginal Offense Columns:")
    print(df_weekly_off_raw.columns.tolist())
    print("\nOriginal Defense shape: ", df_weekly_def_raw.shape)
    print("\nOriginal Defense Columns:")
    print(df_weekly_def_raw.columns.tolist())

    # --- Pre-merge Renaming: Change 'team' to 'team_id' in raw DataFrames ---
    if 'team' in df_weekly_off_raw.columns:
        df_weekly_off_raw.rename(columns={'team': 'team_id'}, inplace=True)
    if 'team' in df_weekly_def_raw.columns:
        df_weekly_def_raw.rename(columns={'team': 'team_id'}, inplace=True)


except FileNotFoundError as e:
    print(f"Error: One of the weekly player CSV files not found. Check paths: {e}")
except Exception as e:
    print(f"An unexpected error occurred during TeamWeeklyStats ETL: {e}")
    raise  # Re-raise the exception after printing for debugging

print("\nETL process for PlayerWeeklyStats completed.")