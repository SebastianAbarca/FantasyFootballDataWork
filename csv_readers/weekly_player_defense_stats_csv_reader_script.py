import pandas as pd
import os

file_names = [
    "weekly_player_stats_defense.csv",
    "weekly_player_stats_offense.csv",
    "weekly_team_stats_defense.csv",
    "weekly_team_stats_offense.csv",
    "yearly_player_stats_defense.csv",
    "yearly_player_stats_offense.csv",
    "yearly_team_stats_defense.csv",
    "yearly_team_stats_offense.csv"
]

# Pandas display settings to prevent truncation
pd.set_option('display.max_columns', None)     # Show all columns
pd.set_option('display.max_rows', None)        # Show all rows (be cautious if dataset is huge)
pd.set_option('display.max_colwidth', None)    # Don't truncate column content
pd.set_option('display.width', 1000)           # Set wide enough console width


base_path = ".."

print(f"\n{'=' * 60}")
print(f"📂 File: {file_names[0]}")
print(f"{'=' * 60}")

file_path = os.path.join(base_path, file_names[0])
flag_one = False #This only needs to be done once. I dont want to make 8 different scripts
try:
    df = pd.read_csv(file_path)
    if(flag_one):
        all_column_names = df.columns.tolist()
        columns_to_drop_by_index = [col_name for idx, col_name in enumerate(all_column_names) if idx >= 34]
        specific_columns = {'fantasy_points_ppr', 'fantasy_points_standard', 'fantasy_temp_player_id'}
        all_columns_to_drop = list(set(columns_to_drop_by_index) | specific_columns)
        existing_columns_to_drop = [col for col in all_columns_to_drop if col in df.columns]
        if existing_columns_to_drop:
            df = df.drop(columns=existing_columns_to_drop)
            print(f"Dropped {len(existing_columns_to_drop)} columns.")
        else:
            print("No columns matched the criteria for dropping or were already absent.")

        print(f"New DataFrame shape: {df.shape}")
        print(f"New DataFrame columns: {df.columns.tolist()}")

        output_csv_name = 'my_player_weekly_stats_defense.csv'
        df.to_csv(output_csv_name, index=False)  # index=False is crucial to avoid writing the DataFrame index
        print(f"Modified DataFrame saved to '{output_csv_name}' successfully.")

    else:
        print("\n🔍 columns:")
        print(df.columns)
        print(df.columns[34])
        columns_greater_than_34_indices = [i for i in range(len(df.columns)) if i >= 34]
        print(columns_greater_than_34_indices)

except Exception as e:
    print(f"⚠️ Error reading {file_names[0]}: {e}")


flag_two = False
file_path_two = os.path.join(base_path, file_names[1])
print(f"\n{'=' * 60}")
print(f"📂 File: {file_names[1]}")
print(f"{'=' * 60}")
try:
    df_two = pd.read_csv(file_path_two)
    if(flag_two):
        specific_columns_two = {'temp_player_id'}
        all_column_names_two = df.columns.tolist()
        columns_to_drop_by_index_two = [col_name for idx, col_name in enumerate(all_column_names) if idx >= 73]
        all_columns_to_drop_two = list(set(columns_to_drop_by_index) | specific_columns)
        existing_columns_to_drop = [col for col in all_columns_to_drop if col in df.columns]
        if existing_columns_to_drop:
            df = df.drop(columns=existing_columns_to_drop)
            print(f"Dropped {len(existing_columns_to_drop)} columns.")
        else:
            print("No columns matched the criteria for dropping or were already absent.")

        print(f"New DataFrame shape: {df.shape}")
        print(f"New DataFrame columns: {df.columns.tolist()}")
        print("TODO")
    else:
        print(df_two.columns[73])

except Exception as e:
    print(f"⚠️ Error reading {file_names[0]}: {e}")
