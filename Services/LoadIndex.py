import os
import pandas as pd

PREDEFINED_WATCHLIST = {}

INDEX_FILENAME = []

def load_csv_files_to_pandas(base_directory=".", sub_directory="Index"):
    """
    Load all CSV files from the specified subdirectory and combine them into a single Pandas DataFrame.
    
    :param base_directory: The base directory where the subdirectory is located.
    :param sub_directory: The name of the subdirectory containing the CSV files.
    :return: A single combined Pandas DataFrame.
    """
    # Define the path to the subdirectory
    index_directory = os.path.join(base_directory, sub_directory)

    # Check if the directory exists
    if not os.path.exists(index_directory):
        print(f"Directory '{index_directory}' not found!")
        return None

    # List to store DataFrames
    dataframes = []

    # Walk through the directory to find CSV files
    for root, _, files in os.walk(index_directory):
        for file in files:
            if file.endswith(".csv"):
                csv_path = os.path.join(root, file)
                try:
                    df = pd.read_csv(csv_path)
                    INDEX_FILENAME.append(os.path.splitext(file)[0])
                    PREDEFINED_WATCHLIST[os.path.splitext(file)[0]] = df.to_dict(orient="records")
                    dataframes.extend(df.to_dict(orient="records"))
                except Exception as e:
                    print(f"Error loading file {csv_path}: {e}")

    # Concatenate all DataFrames into one
    return dataframes