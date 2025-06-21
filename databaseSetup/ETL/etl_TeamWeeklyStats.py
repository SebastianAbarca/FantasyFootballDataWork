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

# --- File Path Configuration ---
cwd = os.getcwd()
parent_dir = os.path.dirname(cwd)
sub_directory = "myData"
file_weekly_offense = 'my_team_weekly_stats_offense.csv'
file_weekly_defense = 'my_team_weekly_stats_defense.csv'

target_folder = os.path.join(parent_dir, sub_directory)
target_file_weekly_offense = os.path.join(target_folder, file_weekly_offense)
target_file_weekly_defense = os.path.join(target_folder, file_weekly_defense)

# --- ETL for TeamWeeklyStats ---
print("\nLoading TeamWeeklyStats...")
try:
    # 1. Read Raw Data
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

    # 2. Define Common Merge Keys
    # These are the keys used to MERGE the offense and defense data.
    # game_id, team_id, season, week, and season_type typically define a unique team-game entry.
    merge_keys = ['game_id', 'team_id', 'season', 'week', 'season_type']

    # 3. Define offense/defense/shared columns based on TeamWeeklyStats CREATE TABLE
    # Note: Column names are taken directly from your CREATE TABLE statement.
    # Adjust these lists if your raw CSVs have different names or if you want to group them differently.

    off_stats_only_cols = [
        'shotgun', 'no_huddle', 'qb_dropback', 'qb_scramble', 'total_off_yards',
        'pass_attempts', 'complete_pass', 'incomplete_pass', 'passing_yards',
        'air_yards', 'receiving_yards', 'yards_after_catch', 'rush_attempts',
        'rushing_yards', 'tackled_for_loss', 'first_down_pass', 'first_down_rush',
        'third_down_converted', 'third_down_failed', 'fourth_down_converted',
        'fourth_down_failed', 'rush_touchdown', 'pass_touchdown', 'receiving_touchdown',
        'total_off_points', 'extra_point', 'field_goal', 'kickoff', 'no_play',
        'pass_snaps', 'punt', 'qb_kneel', 'qb_spike', 'rush_snaps', 'offense_snaps',
        'st_snaps', 'rush_pct', 'pass_pct', 'passing_air_yards', 'receiving_air_yards',
        'receptions', 'targets', 'yps', 'adot', 'air_yards_share', 'target_share',
        'comp_pct', 'int_pct', 'pass_td_pct', 'ypa', 'rec_td_pct', 'yptarget',
        'ayptarget', 'ypr', 'rush_td_pct', 'ypc', 'touches', 'total_tds',
        'td_pct', 'total_yards', 'yptouch'
    ]

    def_stats_only_cols = [
        'solo_tackle', 'assist_tackle', 'tackle_with_assist', 'sack', 'qb_hit',
        'total_def_points', 'defense_snaps'
    ]

    shared_stats_cols = [  # These might appear in both offensive and defensive data conceptually
        'safety', 'interception', 'fumble', 'fumble_lost', 'fumble_forced',
        'fumble_not_forced', 'fumble_out_of_bounds', 'def_touchdown',
        'defensive_two_point_attempt', 'defensive_two_point_conv',
        'defensive_extra_point_attempt', 'defensive_extra_point_conv',
        # Record related columns (assuming these are from one source and might be shared conceptually)
        'home_win', 'home_loss', 'home_tie', 'away_win', 'away_loss', 'away_tie',
        'win', 'loss', 'tie', 'record', 'win_pct'
    ]

    # Select only the merge keys, offense-specific stats, and shared stats for offense DF
    cols_for_off_df = list(set(merge_keys + off_stats_only_cols + shared_stats_cols))
    df_weekly_off = df_weekly_off_raw[[col for col in cols_for_off_df if col in df_weekly_off_raw.columns]].copy()

    # Select only the merge keys, defense-specific stats, and shared stats for defense DF
    cols_for_def_df = list(set(merge_keys + def_stats_only_cols + shared_stats_cols))
    df_weekly_def = df_weekly_def_raw[[col for col in cols_for_def_df if col in df_weekly_def_raw.columns]].copy()

    print("\n--- Performing Outer Merge ---")
    # 4. Perform Outer Merge for all columns. Conflicts will get _x and _y suffixes.
    df_merged_weekly = pd.merge(
        df_weekly_off,
        df_weekly_def,
        on=merge_keys,
        how='outer'
    )

    print(f"Shape after merge: {df_merged_weekly.shape}")
    # print("Columns after merge (note _x and _y suffixes for conflicts):")
    # print(df_merged_weekly.columns.tolist())
    # print("\nSample after merge:")
    # print(df_merged_weekly.head().to_markdown(index=False, numalign="left", stralign="left"))

    # --- Consolidate Shared Numeric Stats (summing _x and _y) ---
    print("\n--- Consolidating Shared Numeric Stats (Summing _x and _y) ---")
    for stat_name in shared_stats_cols:
        off_col = f"{stat_name}_x"
        def_col = f"{stat_name}_y"

        if off_col in df_merged_weekly.columns and def_col in df_merged_weekly.columns:
            # Fill NaNs with 0 before summing to treat missing as 0 contribution
            df_merged_weekly[stat_name] = df_merged_weekly[off_col].fillna(0) + df_merged_weekly[def_col].fillna(0)
            df_merged_weekly.drop(columns=[off_col, def_col], inplace=True)
            # print(f"Consolidated '{off_col}' and '{def_col}' into '{stat_name}' by summing.")
        elif off_col in df_merged_weekly.columns:
            df_merged_weekly.rename(columns={off_col: stat_name}, inplace=True)
            # print(f"Renamed '{off_col}' to '{stat_name}'.")
        elif def_col in df_merged_weekly.columns:
            df_merged_weekly.rename(columns={def_col: stat_name}, inplace=True)
            # print(f"Renamed '{def_col}' to '{stat_name}'.")

    # --- Fill remaining NaN numeric columns with 0.0 ---
    # Get a list of all columns that are not merge_keys
    numeric_cols_to_fill_na = [
        col for col in df_merged_weekly.columns
        if col not in merge_keys and pd.api.types.is_numeric_dtype(df_merged_weekly[col])
    ]

    print(f"\n--- Filling NaN values in numeric stat columns with 0.0 ({len(numeric_cols_to_fill_na)} columns) ---")
    for col in numeric_cols_to_fill_na:
        df_merged_weekly[col] = pd.to_numeric(df_merged_weekly[col], errors='coerce').fillna(0.0)

    # --- Cast numeric columns to appropriate types (int for counts, float for decimals) ---
    print("\n--- Casting numeric columns to appropriate types ---")

    integer_stat_cols = [
        # Merge Keys (handled below with PK casting for consistency)
        # Offensive Stats
        'shotgun', 'no_huddle', 'qb_dropback', 'qb_scramble', 'pass_attempts',
        'complete_pass', 'incomplete_pass', 'rush_attempts', 'tackled_for_loss',
        'first_down_pass', 'first_down_rush', 'third_down_converted',
        'third_down_failed', 'fourth_down_converted', 'fourth_down_failed',
        'rush_touchdown', 'pass_touchdown', 'receiving_touchdown', 'total_off_points',
        'extra_point', 'field_goal', 'kickoff', 'no_play', 'pass_snaps', 'punt',
        'qb_kneel', 'qb_spike', 'rush_snaps', 'offense_snaps', 'st_snaps',
        'receptions', 'targets', 'touches', 'total_tds',
        # Defensive Stats
        'solo_tackle', 'assist_tackle', 'tackle_with_assist', 'qb_hit', 'total_def_points',
        'defense_snaps',
        # Shared Stats
        'safety', 'interception', 'fumble', 'fumble_lost', 'fumble_forced',
        'fumble_not_forced', 'fumble_out_of_bounds', 'def_touchdown',
        'defensive_two_point_attempt', 'defensive_two_point_conv',
        'defensive_extra_point_attempt', 'defensive_extra_point_conv',
        # Record related stats
        'home_win', 'home_loss', 'home_tie', 'away_win', 'away_loss', 'away_tie',
        'win', 'loss', 'tie'
    ]

    if 'sack' in integer_stat_cols:  # Sack is often float, remove if present by mistake
        integer_stat_cols.remove('sack')

    for col in integer_stat_cols:
        if col in df_merged_weekly.columns:
            df_merged_weekly[col] = df_merged_weekly[col].astype(float).fillna(0).astype('Int64')

    float_stat_cols = [
        col for col in df_merged_weekly.columns
        if
        col not in integer_stat_cols and col not in merge_keys and pd.api.types.is_numeric_dtype(df_merged_weekly[col])
    ]
    # Add 'sack' and 'win_pct' explicitly to floats if they exist
    if 'sack' in df_merged_weekly.columns and 'sack' not in float_stat_cols:
        float_stat_cols.append('sack')
    if 'win_pct' in df_merged_weekly.columns and 'win_pct' not in float_stat_cols:
        float_stat_cols.append('win_pct')

    for col in float_stat_cols:
        if col in df_merged_weekly.columns:
            df_merged_weekly[col] = df_merged_weekly[col].astype(float)

    # --- IMPORTANT: Define the primary key columns as they are in your MariaDB table ---
    # Your MariaDB's PRIMARY KEY is: (`game_id`,`team_id`)
    db_primary_key_cols = ['game_id', 'team_id']

    # Ensure core ID columns have no nulls for PK and are string type
    initial_rows_pre_pk_drop = len(df_merged_weekly)
    # Dropna using the actual database primary key components
    df_merged_weekly.dropna(subset=db_primary_key_cols, inplace=True)
    rows_dropped_pk = initial_rows_pre_pk_drop - len(df_merged_weekly)
    if rows_dropped_pk > 0:
        print(
            f"\n--- WARNING: Dropped {rows_dropped_pk} rows due to NULLs in database primary key components ({', '.join(db_primary_key_cols)}). ---")
    else:
        print(f"\n--- Check: No NULLs found in database primary key components. ---")

    # --- Convert all primary key components to string for consistent comparison ---
    for col in db_primary_key_cols:
        if col in df_merged_weekly.columns:
            df_merged_weekly[col] = df_merged_weekly[col].astype(str)
        else:
            raise ValueError(
                f"Primary key column '{col}' not found in df_merged_weekly. Cannot proceed with duplicate check.")

    # --- Deduplicate on the actual database primary key columns *within the DataFrame* ---
    initial_rows_pre_dedupe_df = len(df_merged_weekly)
    internal_duplicates_df = df_merged_weekly[df_merged_weekly.duplicated(subset=db_primary_key_cols, keep=False)]

    if not internal_duplicates_df.empty:
        print(
            f"\n--- CRITICAL: Found {len(internal_duplicates_df)} rows with duplicate primary keys within the DataFrame ({', '.join(db_primary_key_cols)})! ---")
        print("These are the problematic rows (showing first 20):")
        print(internal_duplicates_df.sort_values(by=db_primary_key_cols).head(20).to_markdown(index=False))

        # Drop these duplicates, keeping the first occurrence
        df_merged_weekly.drop_duplicates(subset=db_primary_key_cols, keep='first', inplace=True)
        rows_deduplicated_internal = initial_rows_pre_dedupe_df - len(df_merged_weekly)
        print(f"--- RESOLVED: Dropped {rows_deduplicated_internal} duplicate rows from DataFrame. ---")
    else:
        print(
            f"\n--- PASS: No duplicate primary keys found within the DataFrame ({', '.join(db_primary_key_cols)}). ---")

    print(f"\n--- Total records in DataFrame after internal deduplication: {len(df_merged_weekly)} ---")

    # --- Final Data Integrity Checks for TeamWeeklyStats DataFrame ---
    print(f"\n--- Check: Total records in TeamWeeklyStats after all cleaning: {len(df_merged_weekly)} ---")

    # print("\n--- Check: TeamWeeklyStats DataFrame Info (Final) ---")
    # df_merged_weekly.info(verbose=True, show_counts=True)

    # print("\n--- Check: Null values per column in TeamWeeklyStats DataFrame (Final) ---")
    # print(df_merged_weekly.isnull().sum().to_markdown(numalign="left", stralign="left"))

    # print("\n--- Check: Columns with mixed data types (should ideally be empty) ---")
    mixed_type_columns = []
    for col in df_merged_weekly.columns:
        if pd.api.types.is_object_dtype(df_merged_weekly[col]):
            unique_types = df_merged_weekly[col].dropna().apply(type).unique()
            if len(unique_types) > 1 and not (
                    len(unique_types) == 2 and str in unique_types and np.str_ in unique_types):
                mixed_type_columns.append(f"  - Column '{col}' has mixed types: {unique_types}")
            elif len(unique_types) > 0 and not (
                    pd.api.types.is_string_dtype(df_merged_weekly[col]) or pd.api.types.is_numeric_dtype(
                df_merged_weekly[col])):
                mixed_type_columns.append(f"  - Column '{col}' has unexpected object types: {unique_types}")

    if mixed_type_columns:
        for item in mixed_type_columns:
            print(item)
    else:
        print("  No mixed data types found.")

    print("\n--- Data Integrity Checks Complete for TeamWeeklyStats. Ready for Upload. ---")

    # Debugging Foreign Key Constraint (team_id)
    print("\n--- Debugging Foreign Key Constraint (team_id) ---")

    df_team_ids = df_merged_weekly['team_id'].unique()
    print(f"Total unique team_ids in df_merged_weekly: {len(df_team_ids)}")

    existing_team_ids_set = set()
    try:
        with engine.connect() as connection:
            existing_team_ids_df = pd.read_sql_table('DimTeams', con=connection, columns=['team_id'])
        existing_team_ids_set = set(existing_team_ids_df['team_id'].astype(str).tolist())
        print(f"Total unique team_ids in DimTeams: {len(existing_team_ids_set)}")

        missing_team_ids_in_dim = [tid for tid in df_team_ids if tid not in existing_team_ids_set]

        if missing_team_ids_in_dim:
            print(
                f"\n--- CRITICAL: Found {len(missing_team_ids_in_dim)} team_ids in TeamWeeklyStats that DO NOT exist in DimTeams! ---")
            print(f"Sample missing team_ids: {missing_team_ids_in_dim[:10]}")
            print("Action required: Either pre-populate DimTeams with these IDs or filter them out.")

            original_rows = len(df_merged_weekly)
            df_merged_weekly = df_merged_weekly[df_merged_weekly['team_id'].isin(existing_team_ids_set)].copy()
            rows_filtered_fk = original_rows - len(df_merged_weekly)
            if rows_filtered_fk > 0:
                print(
                    f"--- FILTERED: Dropped {rows_filtered_fk} rows from TeamWeeklyStats because their team_id was not found in DimTeams. ---")
            else:
                print("No rows filtered as all team_ids were found in DimTeams (after initial check).")
        else:
            print("\n--- All team_ids in TeamWeeklyStats exist in DimTeams. Proceeding with upload. ---")

    except Exception as e:
        print(f"Error checking DimTeams: {e}")
        print(
            "Cannot verify team_ids against DimTeams. Proceeding with upload, but be aware of potential FK errors.")

    print(f"\n--- Final records to upload after FK filtering: {len(df_merged_weekly)} ---")

    # --- Pre-upload database duplicate check (Good practice for subsequent runs) ---
    print("\n--- Performing pre-upload duplicate check against database ---")

    # Get existing primary keys from the database
    existing_pks_df = pd.DataFrame(columns=db_primary_key_cols)
    try:
        with engine.connect() as connection:
            # Read only the primary key columns from the existing table
            existing_pks_df = pd.read_sql_table(
                'TeamWeeklyStats',  # Target table name
                con=connection,
                columns=db_primary_key_cols
            )
        print(f"Found {len(existing_pks_df)} existing records in TeamWeeklyStats table.")
    except Exception as e:
        print(f"Warning: Could not read existing primary keys from DB (likely table is empty or new). Error: {e}")

    # Create a unique identifier for merging/comparing based on the DB's actual PK
    df_merged_weekly['db_pk_identifier'] = df_merged_weekly[db_primary_key_cols].agg('-'.join, axis=1)

    if not existing_pks_df.empty:
        # Ensure existing_pks_df columns are also strings for consistent comparison
        for col in db_primary_key_cols:
            existing_pks_df[col] = existing_pks_df[col].astype(str)
        existing_pks_df['db_pk_identifier'] = existing_pks_df[db_primary_key_cols].agg('-'.join, axis=1)

        initial_rows_for_db_check = len(df_merged_weekly)
        df_to_upload_final = df_merged_weekly[
            ~df_merged_weekly['db_pk_identifier'].isin(existing_pks_df['db_pk_identifier'])].copy()
        dropped_db_duplicates_count = initial_rows_for_db_check - len(df_to_upload_final)
        if dropped_db_duplicates_count > 0:
            print(
                f"--- FILTERED: Dropped {dropped_db_duplicates_count} rows because their primary key already exists in the database. ---")
        else:
            print("--- Check: No duplicates found between DataFrame and existing database records. ---")
    else:
        df_to_upload_final = df_merged_weekly.copy()  # No existing records to filter against
        print("--- Database is empty, no records filtered based on existing DB entries. ---")

    # Drop the temporary helper column
    if 'db_pk_identifier' in df_to_upload_final.columns:
        df_to_upload_final = df_to_upload_final.drop(columns=['db_pk_identifier'])

    print(f"\n--- Final records to be uploaded to database: {len(df_to_upload_final)} ---")

    # Attempt to upload only the truly new records
    if not df_to_upload_final.empty:
        df_to_upload_final.to_sql('TeamWeeklyStats', con=engine, if_exists='append', index=False,
                                  chunksize=1000)  # Target table name
        print("\nTeamWeeklyStats data uploaded successfully.")
    else:
        print("\nNo new records to upload to TeamWeeklyStats table.")

except FileNotFoundError as e:
    print(f"Error: One of the weekly team CSV files not found. Check paths: {e}")
except Exception as e:
    print(f"An unexpected error occurred during TeamWeeklyStats ETL: {e}")
    raise  # Re-raise the exception after printing for debugging

print("\nETL process for TeamWeeklyStats completed.")