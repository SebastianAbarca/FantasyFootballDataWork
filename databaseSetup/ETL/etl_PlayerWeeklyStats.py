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
file_weekly_offense = 'my_player_weekly_stats_offense.csv'
file_weekly_defense = 'my_player_weekly_stats_defense.csv'

target_folder = os.path.join(parent_dir, sub_directory)
target_file_weekly_offense = os.path.join(target_folder, file_weekly_offense)
target_file_weekly_defense = os.path.join(target_folder, file_weekly_defense)

# --- ETL for PlayerWeeklyStats ---
print("\nLoading PlayerWeeklyStats...")
try:
    # 1. Read Raw Data
    df_weekly_off_raw = pd.read_csv(target_file_weekly_offense)
    df_weekly_def_raw = pd.read_csv(target_file_weekly_defense)
    # print("\nOriginal Offense shape: ", df_weekly_off_raw.shape)
    # print("\nOriginal Offense Columns:")
    # print(df_weekly_off_raw.columns.tolist())
    # print("\nOriginal Defense shape: ", df_weekly_def_raw.shape)
    # print("\nOriginal Defense Columns:")
    # print(df_weekly_def_raw.columns.tolist())

    # --- Pre-merge Renaming: Change 'team' to 'team_id' in raw DataFrames ---
    if 'team' in df_weekly_off_raw.columns:
        df_weekly_off_raw.rename(columns={'team': 'team_id'}, inplace=True)
    if 'team' in df_weekly_def_raw.columns:
        df_weekly_def_raw.rename(columns={'team': 'team_id'}, inplace=True)

    # 2. Define Common Merge Keys (now using 'team_id')
    # These are the keys used to MERGE the offense and defense data.
    # It's okay for team_id to be here for the merge.
    merge_keys = ['player_id', 'team_id', 'season', 'week']

    # 3. Define offense/defense/shared/descriptive columns season_type handled seperately
    off_stats_only_cols = [
        'shotgun', 'no_huddle', 'qb_dropback', 'qb_scramble', 'pass_attempts',
        'complete_pass', 'incomplete_pass', 'passing_yards', 'receiving_yards',
        'yards_after_catch', 'rush_attempts', 'rushing_yards', 'tackled_for_loss',
        'first_down_pass', 'first_down_rush', 'third_down_converted',
        'third_down_failed', 'fourth_down_converted', 'fourth_down_failed',
        'rush_touchdown', 'pass_touchdown', 'receptions', 'targets', 'passing_air_yards',
        'receiving_air_yards', 'receiving_touchdown', 'fantasy_points_ppr',
        'fantasy_points_standard', 'passer_rating', 'adot', 'air_yards_share',
        'target_share', 'comp_pct', 'int_pct', 'pass_td_pct', 'ypa', 'rec_td_pct',
        'yptarget', 'ayptarget', 'ypr', 'rush_td_pct', 'ypc', 'touches', 'total_tds',
        'td_pct', 'total_yards', 'yptouch', 'offense_snaps', 'offense_pct', 'team_offense_snaps'
    ]

    def_stats_only_cols = [
        'solo_tackle', 'assist_tackle', 'tackle_with_assist', 'sack', 'qb_hit',
        'defense_snaps', 'defense_pct', 'team_defense_snaps'
    ]

    shared_stats_cols = [
        'safety', 'interception', 'fumble', 'fumble_lost', 'fumble_forced',
        'fumble_not_forced', 'fumble_out_of_bounds', 'def_touchdown',
        'defensive_two_point_attempt', 'defensive_two_point_conv',
        'defensive_extra_point_attempt', 'defensive_extra_point_conv'
    ]

    descriptive_player_cols = [
        'player_name', 'position', 'birth_year', 'draft_year',
        'draft_round', 'draft_pick', 'draft_ovr', 'height', 'weight', 'college'
    ]

    # Select only the merge keys, offense-specific stats, and shared stats for offense DF
    cols_for_off_df = list(set(merge_keys + off_stats_only_cols + shared_stats_cols + ['season_type']))
    df_weekly_off = df_weekly_off_raw[[col for col in cols_for_off_df if col in df_weekly_off_raw.columns]].copy()

    # Select only the merge keys, defense-specific stats, and shared stats for defense DF
    cols_for_def_df = list(set(merge_keys + def_stats_only_cols + shared_stats_cols + ['season_type']))
    df_weekly_def = df_weekly_def_raw[[col for col in cols_for_def_df if col in df_weekly_def_raw.columns]].copy()

    # print("\n--- DataFrames Prepared for Merge (All columns, ready for _x/_y conflicts) ---")
    # print("df_weekly_off head:")
    # print(df_weekly_off.head().to_markdown(index=False, numalign="left", stralign="left"))
    # print("\ndf_weekly_off info:")
    # df_weekly_off.info()

    # print("\n\ndf_weekly_def head:")
    # print(df_weekly_def.head().to_markdown(index=False, numalign="left", stralign="left"))
    # print("\ndf_weekly_def info:")
    # df_weekly_def.info()

    # print("\n--- Performing Outer Merge ---")
    # 3. Perform Outer Merge for all columns. Conflicts will get _x and _y suffixes.
    df_merged_weekly = pd.merge(
        df_weekly_off,
        df_weekly_def,
        on=merge_keys,
        how='outer'
    )

    # print(f"Shape after merge: {df_merged_weekly.shape}")
    # print("Columns after merge (note _x and _y suffixes for conflicts):")
    # print(df_merged_weekly.columns.tolist())
    # print("\nSample after merge:")
    # print(df_merged_weekly.head().to_markdown(index=False, numalign="left", stralign="left"))

    # Resolve 'season_type' conflict (Reg > Post)
    # print("\n--- Consolidating 'season_type' (Regular > Postseason preference) ---")

    # Replace NaN with a consistent placeholder for easier comparison for season_type_x and season_type_y
    df_merged_weekly['season_type_x'] = df_merged_weekly['season_type_x'].fillna('')
    df_merged_weekly['season_type_y'] = df_merged_weekly['season_type_y'].fillna('')


    def resolve_season_type(row):
        off_type = str(row['season_type_x']).strip().lower()
        def_type = str(row['season_type_y']).strip().lower()

        if 'regular' in [off_type, def_type]:
            return 'Reg'  # Use 'Reg' to match DB 'Reg' and 'Post'
        elif 'postseason' in [off_type, def_type]:
            return 'Post'  # Use 'Post' to match DB 'Reg' and 'Post'
        elif off_type:  # if only offense has a season type
            return off_type.capitalize()
        elif def_type:  # if only defense has a season type
            return def_type.capitalize()
        else:
            return 'Unknown'  # if both are empty/NaN


    if 'season_type_x' in df_merged_weekly.columns and 'season_type_y' in df_merged_weekly.columns:
        df_merged_weekly['season_type'] = df_merged_weekly.apply(resolve_season_type, axis=1)
        df_merged_weekly.drop(columns=['season_type_x', 'season_type_y'], inplace=True)
        # print("Consolidated 'season_type' from '_x' and '_y' columns.")
    elif 'season_type_x' in df_merged_weekly.columns:
        df_merged_weekly.rename(columns={'season_type_x': 'season_type'}, inplace=True)
        # Handle cases where season_type_x might be NaN for def-only players after outer merge
        df_merged_weekly['season_type'] = df_merged_weekly['season_type'].fillna('Unknown').astype(str).apply(
            lambda x: x.capitalize())
        # print("Renamed 'season_type_x' to 'season_type'.")
    elif 'season_type_y' in df_merged_weekly.columns:
        df_merged_weekly.rename(columns={'season_type_y': 'season_type'}, inplace=True)
        # Handle cases where season_type_y might be NaN for off-only players after outer merge
        df_merged_weekly['season_type'] = df_merged_weekly['season_type'].fillna('Unknown').astype(str).apply(
            lambda x: x.capitalize())
        # print("Renamed 'season_type_y' to 'season_type'.")
    else:
        df_merged_weekly['season_type'] = 'Unknown'  # Fallback if neither exists (unlikely with current data)
        # print("No 'season_type' column found, created 'Unknown' placeholder.")

    # print("Sample of consolidated season_type:")
    # print(df_merged_weekly[['player_id', 'season', 'week', 'season_type']].head().to_markdown(index=False,
    #                                                                                          numalign="left",
    #                                                                                          stralign="left"))

    # --- Consolidate Shared Numeric Stats (summing _x and _y) ---
    # print("\n--- Consolidating Shared Numeric Stats (Summing _x and _y) ---")
    for stat_name in shared_stats_cols:
        off_col = f"{stat_name}_x"
        def_col = f"{stat_name}_y"

        if off_col in df_merged_weekly.columns and def_col in df_merged_weekly.columns:
            # Fill NaNs with 0 before summing to treat missing as 0 contribution
            df_merged_weekly[stat_name] = df_merged_weekly[off_col].fillna(0) + df_merged_weekly[def_col].fillna(0)
            df_merged_weekly.drop(columns=[off_col, def_col], inplace=True)
        #        print(f"Consolidated '{off_col}' and '{def_col}' into '{stat_name}' by summing.")
        elif off_col in df_merged_weekly.columns:
            df_merged_weekly.rename(columns={off_col: stat_name}, inplace=True)
        #        print(f"Renamed '{off_col}' to '{stat_name}'.")
        elif def_col in df_merged_weekly.columns:
            df_merged_weekly.rename(columns={def_col: stat_name}, inplace=True)
    #        print(f"Renamed '{def_col}' to '{stat_name}'.")

    # --- Fill remaining NaN numeric columns with 0.0 ---
    # Get a list of all columns that are not merge_keys or season_type
    numeric_cols_to_fill_na = [
        col for col in df_merged_weekly.columns
        if col not in merge_keys and col != 'season_type' and pd.api.types.is_numeric_dtype(df_merged_weekly[col])
    ]

    # print(f"\n--- Filling NaN values in numeric stat columns with 0.0 ({len(numeric_cols_to_fill_na)} columns) ---")
    for col in numeric_cols_to_fill_na:
        df_merged_weekly[col] = pd.to_numeric(df_merged_weekly[col], errors='coerce').fillna(0.0)
        # Ensure any non-numeric values converted to NaN are now 0.0

    # --- Cast numeric columns to appropriate types (int for counts, float for decimals) ---
    # print("\n--- Casting numeric columns to appropriate types ---")

    # Define columns that should ideally be integers
    integer_stat_cols = [
        # Merge Keys (season, week will be handled below with PK casting for consistency)
        # Offensive Stats (from off_stats_only_cols)
        'shotgun', 'no_huddle', 'qb_dropback', 'qb_scramble', 'pass_attempts',
        'complete_pass', 'incomplete_pass', 'rush_attempts', 'tackled_for_loss',
        'first_down_pass', 'first_down_rush', 'third_down_converted',
        'third_down_failed', 'fourth_down_converted', 'fourth_down_failed',
        'rush_touchdown', 'pass_touchdown', 'receptions', 'targets',
        'receiving_touchdown', 'touches', 'total_tds', 'offense_snaps', 'team_offense_snaps',
        # Defensive Stats (from def_stats_only_cols)
        'solo_tackle', 'assist_tackle', 'tackle_with_assist', 'qb_hit',  # sack handled as float due to potential .5
        'defense_snaps', 'team_defense_snaps',
        # Shared Stats (after consolidation, which are generally counts)
        'safety', 'interception', 'fumble', 'fumble_lost', 'fumble_forced',
        'fumble_not_forced', 'fumble_out_of_bounds', 'def_touchdown',
        'defensive_two_point_attempt', 'defensive_two_point_conv',
        'defensive_extra_point_attempt', 'defensive_extra_point_conv'
    ]

    # 'sack' can be fractional (e.g., 0.5 sacks), so it should generally be float.
    # Ensure it's not in the integer list.
    if 'sack' in integer_stat_cols:
        integer_stat_cols.remove(
            'sack')  # This is a safety check; it should not be there if def_stats_only_cols is defined correctly.

    # Convert to integer where appropriate
    for col in integer_stat_cols:
        if col in df_merged_weekly.columns:
            # Convert to float first to handle NaNs, then to Int64 (pandas nullable integer)
            # then to regular int if desired for DB. Using Int64 for robust NaN handling before DB upload.
            df_merged_weekly[col] = df_merged_weekly[col].astype(float).fillna(0).astype(
                'Int64')  # Using 'Int64' for nullable integer
            # print(f"  - Converted '{col}' to Int64.") # Uncomment for verbose output

    # Ensure other numeric columns are float (e.g., percentages, averages, yards, sack)
    float_stat_cols = [
        col for col in df_merged_weekly.columns
        if
        col not in integer_stat_cols and col not in merge_keys and col != 'season_type' and pd.api.types.is_numeric_dtype(
            df_merged_weekly[col])
    ]
    # Add 'sack' explicitly if it's a column and not already in integer_stat_cols
    if 'sack' in df_merged_weekly.columns and 'sack' not in integer_stat_cols and 'sack' not in float_stat_cols:
        float_stat_cols.append('sack')

    for col in float_stat_cols:
        if col in df_merged_weekly.columns:
            df_merged_weekly[col] = df_merged_weekly[col].astype(float)
            # print(f"  - Converted '{col}' to float.") # Uncomment for verbose output

    # Ensure core ID columns have no nulls for PK and are string type
    initial_rows_pre_pk_drop = len(df_merged_weekly)
    # Dropna using the actual database primary key components
    df_merged_weekly.dropna(subset=['player_id', 'season', 'season_type', 'week'], inplace=True)
    rows_dropped_pk = initial_rows_pre_pk_drop - len(df_merged_weekly)
    if rows_dropped_pk > 0:
        print(
            f"\n--- WARNING: Dropped {rows_dropped_pk} rows due to NULLs in database primary key components (player_id, season, season_type, week). ---")
    else:
        print(f"\n--- Check: No NULLs found in database primary key components after merge. ---")

    # --- IMPORTANT: Correctly define the primary key columns as they are in your MariaDB table ---
    # Your MariaDB's PRIMARY KEY is: (`player_id`,`season`,`season_type`,`week`)
    db_primary_key_cols = ['player_id', 'season', 'season_type', 'week']

    # --- Convert all primary key components to string for consistent comparison ---
    # This is crucial for `duplicated()` and `isin()` to work correctly
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

    # --- Final Data Integrity Checks for PlayerWeeklyStats DataFrame ---
    # print(f"\n--- Check: Total records in PlayerWeeklyStats after all cleaning: {len(df_merged_weekly)} ---")

    # print("\n--- Check: PlayerWeeklyStats DataFrame Info (Final) ---")
    # df_merged_weekly.info(verbose=True, show_counts=True)

    # print("\n--- Check: Null values per column in PlayerWeeklyStats DataFrame (Final) ---")
    # print(df_merged_weekly.isnull().sum().to_markdown(numalign="left", stralign="left"))

    # print("\n--- Check: Columns with mixed data types (should ideally be empty) ---")
    mixed_type_columns = []
    for col in df_merged_weekly.columns:
        if pd.api.types.is_object_dtype(df_merged_weekly[col]):
            unique_types = df_merged_weekly[col].dropna().apply(type).unique()
            # If there's more than one type and it's not just str and numpy.str_ (which are compatible)
            # or if it contains non-string types
            if len(unique_types) > 1 and not (
                    len(unique_types) == 2 and str in unique_types and np.str_ in unique_types):
                mixed_type_columns.append(f"  - Column '{col}' has mixed types: {unique_types}")
            elif len(unique_types) == 1 and (unique_types[0] == str or unique_types[0] == np.str_):
                pass  # This is fine, it's consistent string type
            elif len(unique_types) > 0 and not (
                    pd.api.types.is_string_dtype(df_merged_weekly[col]) or pd.api.types.is_numeric_dtype(
                df_merged_weekly[col])):
                mixed_type_columns.append(f"  - Column '{col}' has unexpected object types: {unique_types}")

    if mixed_type_columns:
        for item in mixed_type_columns:
            print(item)
    else:
        print("  No mixed data types found.")

    # print("\n--- Data Integrity Checks Complete for PlayerWeeklyStats. Ready for Upload. ---")

    # Debugging and conditional filtering for FK issues (DimPlayers) (KEEP THIS!)
    print("\n--- Debugging Foreign Key Constraint (player_id) ---")

    df_player_ids = df_merged_weekly['player_id'].unique()
    print(f"Total unique player_ids in df_merged_weekly: {len(df_player_ids)}")

    existing_player_ids_set = set()
    try:
        with engine.connect() as connection:
            existing_player_ids_df = pd.read_sql_table('DimPlayers', con=connection, columns=['player_id'])
        existing_player_ids_set = set(existing_player_ids_df['player_id'].astype(str).tolist())
        print(f"Total unique player_ids in DimPlayers: {len(existing_player_ids_set)}")

        missing_player_ids_in_dim = [pid for pid in df_player_ids if pid not in existing_player_ids_set]

        if missing_player_ids_in_dim:
            # print(
            #    f"\n--- CRITICAL: Found {len(missing_player_ids_in_dim)} player_ids in PlayerWeeklyStats that DO NOT exist in DimPlayers! ---")
            # print(f"Sample missing player_ids: {missing_player_ids_in_dim[:10]}")
            print("Action required: Either pre-populate DimPlayers with these IDs or filter them out.")

            original_rows = len(df_merged_weekly)
            df_merged_weekly = df_merged_weekly[df_merged_weekly['player_id'].isin(existing_player_ids_set)].copy()
            rows_filtered_fk = original_rows - len(df_merged_weekly)
            if rows_filtered_fk > 0:
                print(
                    f"--- FILTERED: Dropped {rows_filtered_fk} rows from PlayerWeeklyStats because their player_id was not found in DimPlayers. ---")
            else:
                print("No rows filtered as all player_ids were found in DimPlayers (after initial check).")
        else:
            print("\n--- All player_ids in PlayerWeeklyStats exist in DimPlayers. Proceeding with upload. ---")

    except Exception as e:
        print(f"Error checking DimPlayers: {e}")
        print(
            "Cannot verify player_ids against DimPlayers. Proceeding with upload, but be aware of potential FK errors.")

    print(f"\n--- Final records to upload after FK filtering: {len(df_merged_weekly)} ---")

    # --- Pre-upload database duplicate check (Good practice for subsequent runs) ---
    # This step is still valuable for future runs where the DB might not be empty.
    print("\n--- Performing pre-upload duplicate check against database ---")

    # Get existing primary keys from the database
    existing_pks_df = pd.DataFrame(columns=db_primary_key_cols)
    try:
        with engine.connect() as connection:
            # Read only the primary key columns from the existing table
            existing_pks_df = pd.read_sql_table(
                'PlayerWeeklyStats',
                con=connection,
                columns=db_primary_key_cols
            )
        print(f"Found {len(existing_pks_df)} existing records in PlayerWeeklyStats table.")
    except Exception as e:
        print(f"Warning: Could not read existing primary keys from DB (likely table is empty or new). Error: {e}")
        # This block handles the case where the table might not exist yet, or
        # if there's a permission/connection issue, it won't crash the script.

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
        df_to_upload_final.to_sql('PlayerWeeklyStats', con=engine, if_exists='append', index=False, chunksize=1000)
        print("\nPlayerWeeklyStats data uploaded successfully.")
    else:
        print("\nNo new records to upload to PlayerWeeklyStats table.")

except FileNotFoundError as e:
    print(f"Error: One of the weekly player CSV files not found. Check paths: {e}")
except Exception as e:
    print(f"An unexpected error occurred during PlayerWeeklyStats ETL: {e}")
    raise  # Re-raise the exception after printing for debugging

print("\nETL process for PlayerWeeklyStats completed.")