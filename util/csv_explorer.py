import pandas as pd
import os

def explore_csv_columns_in_subdirectory(subdirectory_name="myData"):
    """
    Opens all CSV files in a specified subdirectory within the current working directory
    and prints their column names.

    Args:
        subdirectory_name (str): The name of the subdirectory to explore.
                                 Defaults to "myData".
    """
    current_working_directory = os.getcwd() # Get the absolute path of the current working directory
    target_folder_path = os.path.join(current_working_directory, subdirectory_name)

    print(f"\n{'=' * 60}")
    print(f"🔎 Exploring CSV Columns in: {os.path.abspath(target_folder_path)}")
    print(f"{'=' * 60}")

    if not os.path.isdir(target_folder_path):
        print(f"Error: The directory '{target_folder_path}' does not exist.")
        print("Please ensure the 'myData' folder is directly inside your current working directory.")
        print(f"Current working directory is: {current_working_directory}")
        print(f"\n{'=' * 60}")
        print("Exploration Halted!")
        print(f"{'=' * 60}\n")
        return

    found_csvs = False
    for filename in os.listdir(target_folder_path):
        if filename.endswith('.csv'):
            found_csvs = True
            file_path = os.path.join(target_folder_path, filename)
            print(f"\n--- Columns for: {filename} ---")
            try:
                # Read only a small portion of the file to get column names efficiently
                df = pd.read_csv(file_path, nrows=0)
                if not df.columns.empty:
                    for i, col in enumerate(df.columns):
                        print(f"  [{i}] {col}")
                else:
                    print("  (No columns found or file is empty)")
            except pd.errors.EmptyDataError:
                print("  (File is empty or contains no data rows)")
            except Exception as e:
                print(f"  ⚠️ Error reading file: {e}")

    if not found_csvs:
        print(f"No CSV files found in '{target_folder_path}'.")

    print(f"\n{'=' * 60}")
    print("Exploration Complete!")
    print(f"{'=' * 60}\n")

# --- How to use it ---
# Simply call the function. It defaults to looking for "myData".
explore_csv_columns_in_subdirectory()

# If your folder has a different name, you can specify it:
# explore_csv_columns_in_subdirectory("my_csv_files")