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
# Assuming yearly stats files for teams are named similarly
file_yearly_offense = 'my_team_yearly_stats_offense.csv'
file_yearly_defense = 'my_team_yearly_stats_defense.csv'

target_folder = os.path.join(parent_dir, sub_directory)
target_file_yearly_offense = os.path.join(target_folder, file_yearly_offense)
target_file_yearly_defense = os.path.join(target_folder, file_yearly_defense)

# --- ETL for TeamYearlyStats ---
print("\nLoading TeamYearlyStats...")
try:
    # 1. Read Raw Data
    df_yearly_off_raw = pd.read_csv(target_file_yearly_offense)
    df_yearly_def_raw = pd.read_csv(target_file_yearly_defense)
    print("\nOriginal Offense shape: ", df_yearly_off_raw.shape)
    print("Original Offense Columns:")
    print(df_yearly_off_raw.columns.tolist())
    print("\nOriginal Defense shape: ", df_yearly_def_raw.shape)
    print("Original Defense Columns:")
    print(df_yearly_def_raw.columns.tolist())

    # --- Pre-merge Renaming: Change 'team' to 'team_id' in raw DataFrames if necessary ---
    if 'team' in df_yearly_off_raw.columns:
        df_yearly_off_raw.rename(columns={'team': 'team_id'}, inplace=True)
    if 'team' in df_yearly_def_raw.columns:
        df_yearly_def_raw.rename(columns={'team': 'team_id'}, inplace=True)

    # 2. Define Common Merge Keys
    # For yearly team stats, typically team_id, season, and season_type define uniqueness
    merge_keys = ['team_id', 'season', 'season_type']

    # 3. Define offense/defense/shared columns based on TeamYearlyStats CREATE TABLE
    # These lists should accurately reflect the columns in your raw CSVs
    # AND the target table. Adjust if your raw data names differ.

    off_stats_only_cols = [
        'shotgun', 'no_huddle', 'qb_dropback', 'qb_scramble', 'total_off_yards',
        'pass_attempts', 'complete_pass', 'incomplete_pass', 'passing_yards',
        'air_yards', 'receiving_yards', 'yards_after_catch', 'rush_attempts',
        'rushing_yards', 'tackled_for_loss', 'first_down_pass', 'first_down_rush',
        'third_down_converted', 'third_down_failed', 'fourth_down_converted',
        'fourth_down_failed', 'rush_touchdown', 'pass_touchdown',
        'receiving_touchdown', 'total_off_points', 'offense_snaps',
        'rush_snaps', 'pass_snaps', 'passing_air_yards', 'receiving_air_yards',
        'receptions', 'targets', 'yps', 'adot', 'air_yards_share', 'target_share',
        'comp_pct', 'int_pct', 'pass_td_pct', 'ypa', 'rec_td_pct', 'yptarget',
        'ayptarget', 'ypr', 'rush_td_pct', 'ypc', 'touches', 'total_tds',
        'td_pct', 'total_yards', 'yptouch', 'rush_pct', 'pass_pct' # These are doubles
    ]

    def_stats_only_cols = [
        'solo_tackle', 'assist_tackle', 'tackle_with_assist', 'sack', 'qb_hit',
        'def_touchdown', 'defensive_two_point_attempt', 'defensive_two_point_conv',
        'defensive_extra_point_attempt', 'defensive_extra_point_conv',
        'total_def_points', 'defense_snaps'
    ]

    shared_stats_cols = [ # Record related stats commonly found in one or both
        'win', 'loss', 'tie', 'win_pct' # win_pct is double
    ]


    # Select relevant columns for each DataFrame before merging to avoid unnecessary _x, _y suffixes
    cols_for_off_df = list(set(merge_keys + off_stats_only_cols + shared_stats_cols))
    df_yearly_off = df_yearly_off_raw[[col for col in cols_for_off_df if col in df_yearly_off_raw.columns]].copy()

    cols_for_def_df = list(set(merge_keys + def_stats_only_cols + shared_stats_cols))
    df_yearly_def = df_yearly_def_raw[[col for col in cols_for_def_df if col in df_yearly_def_raw.columns]].copy()


    print("\n--- Performing Outer Merge ---")
    # 4. Perform Outer Merge for all columns. Conflicts will get _x and _y suffixes.
    df_merged_yearly = pd.merge(
        df_yearly_off,
        df_yearly_def,
        on=merge_keys,
        how='outer'
    )

    print(f"Shape after merge: {df_merged_yearly.shape}")


    # --- Consolidate Shared Numeric Stats (summing _x and _y) ---
    print("\n--- Consolidating Shared Stats (Numeric Summing / Non-Numeric Prioritizing) ---")
    for stat_name in shared_stats_cols:
        off_col = f"{stat_name}_x"
        def_col = f"{stat_name}_y"

        if off_col in df_merged_yearly.columns and def_col in df_merged_yearly.columns:
            if pd.api.types.is_numeric_dtype(df_merged_yearly[off_col]) or pd.api.types.is_numeric_dtype(df_merged_yearly[def_col]):
                # Sum numeric columns (e.g., win, loss, tie counts if they appear in both and need summing)
                df_merged_yearly[stat_name] = df_merged_yearly[off_col].fillna(0) + df_merged_yearly[def_col].fillna(0)
            else:
                # For non-numeric or if only one is relevant, prioritize _x if available, else _y
                df_merged_yearly[stat_name] = df_merged_yearly[off_col].fillna(df_merged_yearly[def_col])
            df_merged_yearly.drop(columns=[off_col, def_col], inplace=True)
        elif off_col in df_merged_yearly.columns:
            df_merged_yearly.rename(columns={off_col: stat_name}, inplace=True)
        elif def_col in df_merged_yearly.columns:
            df_merged_yearly.rename(columns={def_col: stat_name}, inplace=True)


    # --- Define all columns expected to be integer or float in the database ---
    integer_db_cols = [
        'season', # PK component
        'shotgun', 'no_huddle', 'qb_dropback', 'qb_scramble', 'pass_attempts',
        'complete_pass', 'incomplete_pass', 'rush_attempts', 'tackled_for_loss',
        'first_down_pass', 'first_down_rush', 'third_down_converted',
        'third_down_failed', 'fourth_down_converted', 'fourth_down_failed',
        'rush_touchdown', 'pass_touchdown', 'receiving_touchdown', 'total_off_points',
        'offense_snaps', 'rush_snaps', 'pass_snaps', 'receptions', 'targets',
        'touches', 'total_tds',
        'solo_tackle', 'assist_tackle', 'tackle_with_assist', 'qb_hit',
        'def_touchdown', 'defensive_two_point_attempt', 'defensive_two_point_conv',
        'defensive_extra_point_attempt', 'defensive_extra_point_conv',
        'total_def_points', 'defense_snaps', 'safety', 'interception', 'fumble',
        'fumble_lost', 'fumble_forced', 'fumble_not_forced', 'fumble_out_of_bounds',
        'win', 'loss', 'tie'
    ]

    float_db_cols = [
        'total_off_yards', 'passing_yards', 'air_yards', 'receiving_yards',
        'yards_after_catch', 'rushing_yards', 'passing_air_yards',
        'receiving_air_yards', 'yps', 'adot', 'air_yards_share', 'target_share',
        'comp_pct', 'int_pct', 'pass_td_pct', 'ypa', 'rec_td_pct', 'yptarget',
        'ayptarget', 'ypr', 'rush_td_pct', 'ypc', 'td_pct', 'total_yards',
        'yptouch', 'sack', 'win_pct', 'rush_pct', 'pass_pct'
    ]

    print("\n--- Casting numeric columns to appropriate types ---")
    for col in df_merged_yearly.columns:
        if col in integer_db_cols:
            try:
                df_merged_yearly[col] = pd.to_numeric(df_merged_yearly[col], errors='coerce').fillna(0).astype('Int64')
            except Exception as e:
                print(f"Error casting '{col}' to Int64. Data type before cast: {df_merged_yearly[col].dtype}")
                print(f"Sample values (first 5): {df_merged_yearly[col].head().tolist()}")
                raise e # Re-raise for debugging
        elif col in float_db_cols:
            try:
                df_merged_yearly[col] = pd.to_numeric(df_merged_yearly[col], errors='coerce').fillna(0.0).astype(float)
            except Exception as e:
                print(f"Error casting '{col}' to float. Data type before cast: {df_merged_yearly[col].dtype}")
                print(f"Sample values (first 5): {df_merged_yearly[col].head().tolist()}")
                raise e # Re-raise for debugging


    # --- IMPORTANT: Define the primary key columns as they are in your MariaDB table ---
    # Your MariaDB's PRIMARY KEY is: (`team_id`,`season`,`season_type`)
    db_primary_key_cols = ['team_id', 'season', 'season_type']

    # Ensure core ID columns have no nulls for PK
    initial_rows_pre_pk_drop = len(df_merged_yearly)
    df_merged_yearly.dropna(subset=db_primary_key_cols, inplace=True)
    rows_dropped_pk = initial_rows_pre_pk_drop - len(df_merged_yearly)
    if rows_dropped_pk > 0:
        print(
            f"\n--- WARNING: Dropped {rows_dropped_pk} rows due to NULLs in database primary key components ({', '.join(db_primary_key_cols)}). ---")
    else:
        print(f"\n--- Check: No NULLs found in database primary key components. ---")


    # --- Convert all primary key components to string for consistent comparison ---
    for col in db_primary_key_cols:
        if col in df_merged_yearly.columns:
            df_merged_yearly[col] = df_merged_yearly[col].astype(str)
        else:
            raise ValueError(f"Primary key column '{col}' not found in df_merged_yearly. Cannot proceed with duplicate check.")

    # --- Deduplicate on the actual database primary key columns *within the DataFrame* ---
    initial_rows_pre_dedupe_df = len(df_merged_yearly)
    internal_duplicates_df = df_merged_yearly[df_merged_yearly.duplicated(subset=db_primary_key_cols, keep=False)]

    if not internal_duplicates_df.empty:
        print(f"\n--- CRITICAL: Found {len(internal_duplicates_df)} rows with duplicate primary keys within the DataFrame ({', '.join(db_primary_key_cols)})! ---")
        print("These are the problematic rows (showing first 20):")
        print(internal_duplicates_df.sort_values(by=db_primary_key_cols).head(20).to_markdown(index=False))

        # Drop these duplicates, keeping the first occurrence
        df_merged_yearly.drop_duplicates(subset=db_primary_key_cols, keep='first', inplace=True)
        rows_deduplicated_internal = initial_rows_pre_dedupe_df - len(df_merged_yearly)
        print(f"--- RESOLVED: Dropped {rows_deduplicated_internal} duplicate rows from DataFrame. ---")
    else:
        print(f"\n--- PASS: No duplicate primary keys found within the DataFrame ({', '.join(db_primary_key_cols)}). ---")

    print(f"\n--- Total records in DataFrame after internal deduplication: {len(df_merged_yearly)} ---")

    # --- Final Data Integrity Checks for TeamYearlyStats DataFrame ---
    print(f"\n--- Check: Total records in TeamYearlyStats after all cleaning: {len(df_merged_yearly)} ---")

    mixed_type_columns = []
    for col in df_merged_yearly.columns:
        if pd.api.types.is_object_dtype(df_merged_yearly[col]):
            unique_types = df_merged_yearly[col].dropna().apply(type).unique()
            if len(unique_types) > 1 and not (
                    len(unique_types) == 2 and str in unique_types and np.str_ in unique_types):
                mixed_type_columns.append(f"  - Column '{col}' has mixed types: {unique_types}")
            elif len(unique_types) > 0 and not (
                    pd.api.types.is_string_dtype(df_merged_yearly[col]) or pd.api.types.is_numeric_dtype(
                    df_merged_yearly[col])):
                mixed_type_columns.append(f"  - Column '{col}' has unexpected object types: {unique_types}")

    if mixed_type_columns:
        print("\n--- WARNING: Columns with mixed data types found! ---")
        for item in mixed_type_columns:
            print(item)
        print("These may cause issues with database insertion if not explicitly handled.")
    else:
        print("  No mixed data types found.")

    print("\n--- Data Integrity Checks Complete for TeamYearlyStats. Ready for Upload. ---")

    # Debugging Foreign Key Constraint (team_id)
    print("\n--- Debugging Foreign Key Constraint (team_id) ---")

    df_team_ids = df_merged_yearly['team_id'].unique()
    print(f"Total unique team_ids in df_merged_yearly: {len(df_team_ids)}")

    existing_team_ids_set = set()
    try:
        with engine.connect() as connection:
            existing_team_ids_df = pd.read_sql_table('DimTeams', con=connection, columns=['team_id'])
        existing_team_ids_set = set(existing_team_ids_df['team_id'].astype(str).tolist())
        print(f"Total unique team_ids in DimTeams: {len(existing_team_ids_set)}")

        missing_team_ids_in_dim = [tid for tid in df_team_ids if tid not in existing_team_ids_set]

        if missing_team_ids_in_dim:
            print(f"\n--- CRITICAL: Found {len(missing_team_ids_in_dim)} team_ids in TeamYearlyStats that DO NOT exist in DimTeams! ---")
            print(f"Sample missing team_ids: {missing_team_ids_in_dim[:10]}")
            print("Action required: Either pre-populate DimTeams with these IDs or filter them out.")

            original_rows = len(df_merged_yearly)
            df_merged_yearly = df_merged_yearly[df_merged_yearly['team_id'].isin(existing_team_ids_set)].copy()
            rows_filtered_fk = original_rows - len(df_merged_yearly)
            if rows_filtered_fk > 0:
                print(
                    f"--- FILTERED: Dropped {rows_filtered_fk} rows from TeamYearlyStats because their team_id was not found in DimTeams. ---")
            else:
                print("No rows filtered as all team_ids were found in DimTeams (after initial check).")
        else:
            print("\n--- All team_ids in TeamYearlyStats exist in DimTeams. Proceeding with upload. ---")

    except Exception as e:
        print(f"Error checking DimTeams: {e}")
        print(
            "Cannot verify team_ids against DimTeams. Proceeding with upload, but be aware of potential FK errors.")

    print(f"\n--- Final records to upload after all FK filtering: {len(df_merged_yearly)} ---")


    # --- Pre-upload database duplicate check (Good practice for subsequent runs) ---
    print("\n--- Performing pre-upload duplicate check against database ---")

    # Get existing primary keys from the database
    existing_pks_df = pd.DataFrame(columns=db_primary_key_cols)
    try:
        with engine.connect() as connection:
            # Read only the primary key columns from the existing table
            existing_pks_df = pd.read_sql_table(
                'TeamYearlyStats', # Target table name
                con=connection,
                columns=db_primary_key_cols
            )
        print(f"Found {len(existing_pks_df)} existing records in TeamYearlyStats table.")
    except Exception as e:
        print(f"Warning: Could not read existing primary keys from DB (likely table is empty or new). Error: {e}")

    # Create a unique identifier for merging/comparing based on the DB's actual PK
    df_merged_yearly['db_pk_identifier'] = df_merged_yearly[db_primary_key_cols].agg('-'.join, axis=1)

    if not existing_pks_df.empty:
        # Ensure existing_pks_df columns are also strings for consistent comparison
        for col in db_primary_key_cols:
            existing_pks_df[col] = existing_pks_df[col].astype(str)
        existing_pks_df['db_pk_identifier'] = existing_pks_df[db_primary_key_cols].agg('-'.join, axis=1)

        initial_rows_for_db_check = len(df_merged_yearly)
        df_to_upload_final = df_merged_yearly[~df_merged_yearly['db_pk_identifier'].isin(existing_pks_df['db_pk_identifier'])].copy()
        dropped_db_duplicates_count = initial_rows_for_db_check - len(df_to_upload_final)
        if dropped_db_duplicates_count > 0:
            print(f"--- FILTERED: Dropped {dropped_db_duplicates_count} rows because their primary key already exists in the database. ---")
        else:
            print("--- Check: No duplicates found between DataFrame and existing database records. ---")
    else:
        df_to_upload_final = df_merged_yearly.copy() # No existing records to filter against
        print("--- Database is empty, no records filtered based on existing DB entries. ---")

    # Drop the temporary helper column
    if 'db_pk_identifier' in df_to_upload_final.columns:
        df_to_upload_final = df_to_upload_final.drop(columns=['db_pk_identifier'])

    print(f"\n--- Final records to be uploaded to database: {len(df_to_upload_final)} ---")

    # Attempt to upload only the truly new records
    if not df_to_upload_final.empty:
        df_to_upload_final.to_sql('TeamYearlyStats', con=engine, if_exists='append', index=False, chunksize=1000) # Target table name
        print("\nTeamYearlyStats data uploaded successfully.")
    else:
        print("\nNo new records to upload to TeamYearlyStats table.")

except FileNotFoundError as e:
    print(f"Error: One of the yearly team CSV files not found. Check paths: {e}")
except Exception as e:
    print(f"An unexpected error occurred during TeamYearlyStats ETL: {e}")
    raise # Re-raise the exception after printing for debugging

print("\nETL process for TeamYearlyStats completed.")