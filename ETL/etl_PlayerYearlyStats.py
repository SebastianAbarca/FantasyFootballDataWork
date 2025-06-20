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
# Assuming yearly stats files are named similarly but for yearly aggregation
file_yearly_offense = 'my_player_yearly_stats_offense.csv'
file_yearly_defense = 'my_player_yearly_stats_defense.csv'

target_folder = os.path.join(parent_dir, sub_directory)
target_file_yearly_offense = os.path.join(target_folder, file_yearly_offense)
target_file_yearly_defense = os.path.join(target_folder, file_yearly_defense)

# --- ETL for PlayerYearlyStats ---
print("\nLoading PlayerYearlyStats...")
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

    # --- Pre-merge Renaming: Change 'team' to 'team_id' in raw DataFrames ---
    # And player_id for consistency if needed, though usually player_id is fine.
    if 'team' in df_yearly_off_raw.columns:
        df_yearly_off_raw.rename(columns={'team': 'team_id'}, inplace=True)
    if 'team' in df_yearly_def_raw.columns:
        df_yearly_def_raw.rename(columns={'team': 'team_id'}, inplace=True)

    # 2. Define Common Merge Keys
    # These are the keys used to MERGE the offense and defense data.
    # For yearly stats, typically player_id, season, season_type, and team_id
    merge_keys = ['player_id', 'season', 'season_type', 'team_id']

    # 3. Define offense/defense/shared columns based on PlayerYearlyStats CREATE TABLE
    # Note: These lists are based on your CREATE TABLE and common sense for football stats.
    # If your raw CSVs have different column names or meanings, adjust these lists.
    off_stats_only_cols = [
        'shotgun', 'no_huddle', 'qb_dropback', 'qb_scramble', 'pass_attempts',
        'complete_pass', 'incomplete_pass', 'passing_yards', 'receiving_yards',
        'yards_after_catch', 'rush_attempts', 'rushing_yards', 'tackled_for_loss',
        'first_down_pass', 'first_down_rush', 'third_down_converted',
        'third_down_failed', 'fourth_down_converted', 'fourth_down_failed',
        'rush_touchdown', 'pass_touchdown', 'receiving_touchdown', 'receptions',
        'targets', 'passing_air_yards', 'receiving_air_yards',
        'fantasy_points_ppr', 'fantasy_points_standard', # These are doubles in DB
        'total_tds', 'touches', 'total_yards', # total_yards is double in DB
        'offense_snaps', 'team_offense_snaps', 'offense_pct' # offense_pct is double
    ]

    def_stats_only_cols = [
        'solo_tackle', 'assist_tackle', 'tackle_with_assist', 'sack', 'qb_hit', # sack is double in DB
        'defense_snaps', 'team_defense_snaps', 'defense_pct' # defense_pct is double
    ]

    # These are stats that might appear in both offense and defense contexts, or general player attributes
    shared_stats_cols = [
        'safety', 'interception', 'fumble', 'fumble_lost', 'fumble_forced',
        'fumble_not_forced', 'fumble_out_of_bounds', 'def_touchdown',
        'defensive_two_point_attempt', 'defensive_two_point_conv',
        'defensive_extra_point_attempt', 'defensive_extra_point_conv',
        'age', # 'age' is a general player attribute, common to both yearly offense/defense context
        # Player bio/draft info are typically unique per player, but can appear in both files
        'player_name', 'position', 'birth_year', 'draft_year', 'draft_round',
        'draft_pick', 'draft_ovr', 'height', 'weight', 'college'
    ]

    # Select relevant columns for each DataFrame before merging to avoid unnecessary _x, _y suffixes
    # Make sure 'player_id', 'player_name', 'position', 'college', etc. are handled.
    # We will pick non-merge_keys from the raw data that are in our target table's schema.
    # Player bio info (name, position, birth_year etc.) usually comes from one source or is consistent.
    # Assuming offense file might have more complete player bio details.

    cols_for_off_df = list(set(merge_keys + off_stats_only_cols + shared_stats_cols))
    # Filter columns to only those actually present in the DataFrame
    df_yearly_off = df_yearly_off_raw[[col for col in cols_for_off_df if col in df_yearly_off_raw.columns]].copy()

    cols_for_def_df = list(set(merge_keys + def_stats_only_cols + shared_stats_cols))
    # Filter columns to only those actually present in the DataFrame
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
    # print("Columns after merge (note _x and _y suffixes for conflicts):")
    # print(df_merged_yearly.columns.tolist())

    # --- Consolidate Shared Numeric Stats (summing _x and _y) ---
    # For columns like 'safety', 'interception', 'fumble', etc., sum them if they appear in both.
    # For descriptive player info like 'player_name', 'position', 'college', 'birth_year', 'age' etc.
    # use coalesce (take non-null value, preferring _x) or ensure they are identical.
    # For simplicity and given typical data, if both exist and are not NaN, _x (offense) should be primary.
    # However, for things like birth_year, draft_year, they *must* be the same.
    print("\n--- Consolidating Shared Stats ---")
    for stat_name in shared_stats_cols:
        off_col = f"{stat_name}_x"
        def_col = f"{stat_name}_y"

        if off_col in df_merged_yearly.columns and def_col in df_merged_yearly.columns:
            # Handle numeric shared stats by summing them (e.g., safety, interception, fumble counts)
            if pd.api.types.is_numeric_dtype(df_merged_yearly[off_col]) or pd.api.types.is_numeric_dtype(df_merged_yearly[def_col]):
                df_merged_yearly[stat_name] = df_merged_yearly[off_col].fillna(0) + df_merged_yearly[def_col].fillna(0)
            else:
                # For non-numeric shared stats (like player_name, position, college), prioritize _x, then _y
                # Or ensure they are consistent. For now, prefer _x if available, else _y.
                df_merged_yearly[stat_name] = df_merged_yearly[off_col].fillna(df_merged_yearly[def_col])

            df_merged_yearly.drop(columns=[off_col, def_col], inplace=True)
            # print(f"Consolidated '{off_col}' and '{def_col}' into '{stat_name}'.")
        elif off_col in df_merged_yearly.columns:
            df_merged_yearly.rename(columns={off_col: stat_name}, inplace=True)
            # print(f"Renamed '{off_col}' to '{stat_name}'.")
        elif def_col in df_merged_yearly.columns:
            df_merged_yearly.rename(columns={def_col: stat_name}, inplace=True)
            # print(f"Renamed '{def_col}' to '{stat_name}'.")


    # --- Define all columns expected to be integer or float in the database ---
    # This is critical for robust type casting.
    integer_db_cols = [
        'season', 'shotgun', 'no_huddle', 'qb_dropback', 'qb_scramble',
        'pass_attempts', 'complete_pass', 'incomplete_pass', 'rush_attempts',
        'tackled_for_loss', 'first_down_pass', 'first_down_rush',
        'third_down_converted', 'third_down_failed', 'fourth_down_converted',
        'fourth_down_failed', 'rush_touchdown', 'pass_touchdown',
        'receiving_touchdown', 'receptions', 'targets', 'total_tds', 'touches',
        'offense_snaps', 'team_offense_snaps',
        'solo_tackle', 'assist_tackle', 'tackle_with_assist', 'qb_hit',
        'defense_snaps', 'team_defense_snaps',
        'age', 'safety', 'interception', 'fumble', 'fumble_lost',
        'fumble_forced', 'fumble_not_forced', 'fumble_out_of_bounds',
        'def_touchdown', 'defensive_two_point_attempt', 'defensive_two_point_conv',
        'defensive_extra_point_attempt', 'defensive_extra_point_conv',
        'birth_year', 'draft_year', 'draft_round', 'draft_pick', 'draft_ovr',
        'height', 'weight'
    ]

    float_db_cols = [
        'passing_yards', 'receiving_yards', 'yards_after_catch', 'rushing_yards',
        'passing_air_yards', 'receiving_air_yards', 'fantasy_points_ppr',
        'fantasy_points_standard', 'total_yards', 'offense_pct', 'sack', 'defense_pct'
    ]

    print("\n--- Casting numeric columns to appropriate types ---")
    for col in df_merged_yearly.columns:
        if col in integer_db_cols:
            try:
                # Convert to numeric, coercing errors to NaN, then fill NaN with 0, then cast to nullable int
                df_merged_yearly[col] = pd.to_numeric(df_merged_yearly[col], errors='coerce').fillna(0).astype('Int64')
            except Exception as e:
                print(f"Error casting '{col}' to Int64. Data type before cast: {df_merged_yearly[col].dtype}")
                print(f"Sample values (first 5): {df_merged_yearly[col].head().tolist()}")
                raise e # Re-raise the exception to stop execution and debug
        elif col in float_db_cols:
            try:
                # Convert to numeric, coercing errors to NaN, then fill NaN with 0, then cast to float
                df_merged_yearly[col] = pd.to_numeric(df_merged_yearly[col], errors='coerce').fillna(0.0).astype(float)
            except Exception as e:
                print(f"Error casting '{col}' to float. Data type before cast: {df_merged_yearly[col].dtype}")
                print(f"Sample values (first 5): {df_merged_yearly[col].head().tolist()}")
                raise e # Re-raise the exception to stop execution and debug


    # --- IMPORTANT: Define the primary key columns as they are in your MariaDB table ---
    # Your MariaDB's PRIMARY KEY is: (`player_id`,`season`,`season_type`)
    db_primary_key_cols = ['player_id', 'season', 'season_type']

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
    # Season needs to be a string here for PK comparison, even if it's an int in DB
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

    # --- Final Data Integrity Checks for PlayerYearlyStats DataFrame ---
    print(f"\n--- Check: Total records in PlayerYearlyStats after all cleaning: {len(df_merged_yearly)} ---")

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

    print("\n--- Data Integrity Checks Complete for PlayerYearlyStats. Ready for Upload. ---")

    # Debugging Foreign Key Constraint (player_id)
    print("\n--- Debugging Foreign Key Constraint (player_id) ---")

    df_player_ids = df_merged_yearly['player_id'].unique()
    print(f"Total unique player_ids in df_merged_yearly: {len(df_player_ids)}")

    existing_player_ids_set = set()
    try:
        with engine.connect() as connection:
            existing_player_ids_df = pd.read_sql_table('DimPlayers', con=connection, columns=['player_id'])
        existing_player_ids_set = set(existing_player_ids_df['player_id'].astype(str).tolist())
        print(f"Total unique player_ids in DimPlayers: {len(existing_player_ids_set)}")

        missing_player_ids_in_dim = [pid for pid in df_player_ids if pid not in existing_player_ids_set]

        if missing_player_ids_in_dim:
            print(f"\n--- CRITICAL: Found {len(missing_player_ids_in_dim)} player_ids in PlayerYearlyStats that DO NOT exist in DimPlayers! ---")
            print(f"Sample missing player_ids: {missing_player_ids_in_dim[:10]}")
            print("Action required: Either pre-populate DimPlayers with these IDs or filter them out.")

            original_rows = len(df_merged_yearly)
            df_merged_yearly = df_merged_yearly[df_merged_yearly['player_id'].isin(existing_player_ids_set)].copy()
            rows_filtered_fk = original_rows - len(df_merged_yearly)
            if rows_filtered_fk > 0:
                print(
                    f"--- FILTERED: Dropped {rows_filtered_fk} rows from PlayerYearlyStats because their player_id was not found in DimPlayers. ---")
            else:
                print("No rows filtered as all player_ids were found in DimPlayers (after initial check).")
        else:
            print("\n--- All player_ids in PlayerYearlyStats exist in DimPlayers. Proceeding with upload. ---")

    except Exception as e:
        print(f"Error checking DimPlayers: {e}")
        print(
            "Cannot verify player_ids against DimPlayers. Proceeding with upload, but be aware of potential FK errors.")

    print(f"\n--- Final records to upload after player_id FK filtering: {len(df_merged_yearly)} ---")


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
            print(f"\n--- CRITICAL: Found {len(missing_team_ids_in_dim)} team_ids in PlayerYearlyStats that DO NOT exist in DimTeams! ---")
            print(f"Sample missing team_ids: {missing_team_ids_in_dim[:10]}")
            print("Action required: Either pre-populate DimTeams with these IDs or filter them out.")

            original_rows = len(df_merged_yearly)
            df_merged_yearly = df_merged_yearly[df_merged_yearly['team_id'].isin(existing_team_ids_set)].copy()
            rows_filtered_fk = original_rows - len(df_merged_yearly)
            if rows_filtered_fk > 0:
                print(
                    f"--- FILTERED: Dropped {rows_filtered_fk} rows from PlayerYearlyStats because their team_id was not found in DimTeams. ---")
            else:
                print("No rows filtered as all team_ids were found in DimTeams (after initial check).")
        else:
            print("\n--- All team_ids in PlayerYearlyStats exist in DimTeams. Proceeding with upload. ---")

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
                'PlayerYearlyStats', # Target table name
                con=connection,
                columns=db_primary_key_cols
            )
        print(f"Found {len(existing_pks_df)} existing records in PlayerYearlyStats table.")
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
        dropped_db_duplicates_count = initial_rows_for_db_check - len(df_to_upload_final) # Corrected variable name
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

    columns_to_drop_from_yearly_stats = [
        'player_name', 'position', 'birth_year', 'draft_year', 'draft_round',
        'draft_pick', 'draft_ovr', 'height', 'weight', 'college'
    ]

    # Filter out only columns that actually exist in the DataFrame to avoid errors
    existing_cols_to_drop = [col for col in columns_to_drop_from_yearly_stats if col in df_to_upload_final.columns]

    if existing_cols_to_drop:
        df_to_upload_final.drop(columns=existing_cols_to_drop, inplace=True)
        print(f"\n--- Dropped columns not in PlayerYearlyStats table: {existing_cols_to_drop} ---")
    else:
        print("\n--- No extra columns found in DataFrame that needed to be dropped for PlayerYearlyStats table. ---")

    print(f"\n--- Final DataFrame columns for upload: {df_to_upload_final.columns.tolist()} ---")
    # Attempt to upload only the truly new records
    if not df_to_upload_final.empty:
        df_to_upload_final.to_sql('PlayerYearlyStats', con=engine, if_exists='append', index=False, chunksize=1000) # Target table name
        print("\nPlayerYearlyStats data uploaded successfully.")
    else:
        print("\nNo new records to upload to PlayerYearlyStats table.")

except FileNotFoundError as e:
    print(f"Error: One of the yearly player CSV files not found. Check paths: {e}")
except Exception as e:
    print(f"An unexpected error occurred during PlayerYearlyStats ETL: {e}")
    raise # Re-raise the exception after printing for debugging

print("\nETL process for PlayerYearlyStats completed.")