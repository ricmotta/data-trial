import os
import pandas as pd

# Function to load parquet files
def load_parquet_data(path, entity):
    """
    Loads all .parquet files for a given entity from a specified directory (SILVER or GOLD).
    
    Args:
    path (str): Base path where the .parquet files are located (e.g., './SILVER/' or './GOLD/').
    entity (str): The name of the entity whose files will be loaded.
    
    Returns:
    pandas.DataFrame: DataFrame containing all the concatenated data for the entity.
    """
    full_path = os.path.join(path, entity)
    files = [f for f in os.listdir(full_path) if f.endswith('.parquet')]
    df_list = [pd.read_parquet(os.path.join(full_path, file)) for file in files]
    
    # Concatenate all DataFrames
    df_combined = pd.concat(df_list, ignore_index=True)
    
    return df_combined
