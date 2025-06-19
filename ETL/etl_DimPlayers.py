import pandas as pd
from sqlalchemy import create_engine, text
import pymysql as mysql
import os
from dotenv import load_dotenv


def assign_offense_defense_flag(position):
    if pd.isna(position):
        return None  # Handle NaN positions
    position = str(position).upper()
    if position in ['QB', 'RB', 'WR', 'TE', 'FB', 'C', 'G', 'T', 'OT', 'OG', 'OC', 'LT', 'LG', 'RT', 'RG', 'XX']:
        return 'OFF'
    elif position in ['DE', 'DT', 'NT', 'LB', 'CB', 'S', 'FS', 'SS', 'DB', 'DL', 'EDGE', 'MLB', 'OLB', 'ILB']:
        return 'DEF'
    elif position in [ 'P', 'PN', 'LS']:  #Punter, Long Snapper
        return 'ST'  # Special Teams
    elif position in ['K', 'PK']:
        return 'K'
    else:
        return position  # Or 'UNK' for unknown/other

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
file_offense = 'my_player_weekly_stats_offense.csv'
file_defense = 'my_player_weekly_stats_defense.csv'
target_folder = os.path.join(parent_dir, sub_directory)
target_file_offense = os.path.join(target_folder, file_offense)
target_file_defense = os.path.join(target_folder, file_defense)

try:
    df_offense = pd.read_csv(target_file_offense)
    df_offense_players = df_offense[[
        'player_id', 'player_name', 'position', 'birth_year', 'draft_year',
        'draft_round', 'draft_pick', 'draft_ovr', 'height', 'weight', 'college'
    ]].copy()  # Use .copy() to avoid SettingWithCopyWarning
    df_offense_players['source_flag'] = 'OFF'  # Temporary flag for this source

    df_defense = pd.read_csv(target_file_defense)
    df_defense_players = df_defense[[
        'player_id', 'player_name', 'position', 'birth_year', 'draft_year',
        'draft_round', 'draft_pick', 'draft_ovr', 'height', 'weight', 'college'
    ]].copy() # Use .copy() to avoid SettingWithCopyWarning
    df_defense_players['source_flag'] = 'DEF' # Temporary flag for this source

    df_all_players_raw = pd.concat([df_offense_players, df_defense_players], ignore_index=True)
    df_dim_players = df_all_players_raw.drop_duplicates(subset=['player_id'], keep='first')

    df_dim_players['offense_defense_flag'] = df_dim_players['position'].apply(assign_offense_defense_flag)

    for col in ['birth_year', 'draft_year', 'draft_round', 'draft_pick', 'draft_ovr']:
        if col in df_dim_players.columns:
            df_dim_players[col] = pd.to_numeric(df_dim_players[col], errors='coerce').fillna(0).astype(int)
    for col in ['height', 'weight']:
        if col in df_dim_players.columns:
            df_dim_players[col] = pd.to_numeric(df_dim_players[col], errors='coerce').fillna(0.0)

    df_dim_players = df_dim_players.drop(columns=['source_flag'])

    #drop rows with missing data. there were only 29
    initial_rows = len(df_dim_players)
    df_dim_players.dropna(subset=['player_name', 'position'], inplace=True)
    rows_dropped = initial_rows - len(df_dim_players)
    if rows_dropped > 0:
        print(f"\n--- INFO: Dropped {rows_dropped} rows due to NULL 'player_name' or 'position'. ---")
    else:
        print("\n--- INFO: No rows dropped based on NULL 'player_name' or 'position'. ---")

    print("\n--- Inspecting Player(s) with 'XX' position ---")
    players_with_xx_position = df_dim_players[df_dim_players['position'].astype(str).str.upper() == 'XX']
    print(players_with_xx_position) ##there was only 1. It is Aaron Hernandez I have no idea why
    ##check data
    print("\n--- Check: Final DimPlayers DataFrame Info ---")
    df_dim_players.info()

    print("\n--- Check: Null values per column in final DimPlayers DataFrame ---")
    print(df_dim_players.isnull().sum())

    print("\n--- Check: Distribution of 'offense_defense_flag' ---")
    print(df_dim_players['offense_defense_flag'].value_counts(dropna=False))  # Include NaNs in count


    print("\n--- Data Integrity Checks Complete for DimPlayers. Ready for Upload. ---")

    df_dim_players.to_sql('DimPlayers', con=engine, if_exists='append', index=False, chunksize=1000)
    print("DimPlayers loaded successfully.")

except FileNotFoundError as e:
    print(f"Error: One of the player CSV files not found. Check paths: {e}")
except Exception as e:
    if "Duplicate entry" in str(e) and "for key 'PRIMARY'" in str(e):
        print("DimPlayers already contains these entries. Skipping insertion for existing players.")
    else:
        print(f"An unexpected error occurred during DimPlayers ETL: {e}")

