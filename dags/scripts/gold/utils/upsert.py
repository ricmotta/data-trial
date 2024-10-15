import os
import pandas as pd

# Function to perform UPSERT/MERGE into GOLD layer
def upsert_to_gold(gold_file, df_new, entity, silver_file_mtime):
    """
    Performs UPSERT/MERGE into the GOLD layer based on modification times of SILVER files.
    Updates or inserts new data into GOLD if SILVER data is newer.
    
    Args:
    gold_file (str): Path to the GOLD parquet file
    df_new (pandas.DataFrame): DataFrame from SILVER
    entity (str): Entity name (used to determine primary key)
    silver_file_mtime (float): Modification time of SILVER file
    """
    gold_path = os.path.join(os.path.dirname(gold_file), entity)
    os.makedirs(gold_path, exist_ok=True)

    full_gold_file = os.path.join(gold_path, os.path.basename(gold_file))

    if os.path.exists(full_gold_file):
        df_gold = pd.read_parquet(full_gold_file)
        gold_file_mtime = os.path.getmtime(full_gold_file)

        if silver_file_mtime > gold_file_mtime:
            print(f"SILVER data is newer. Performing UPSERT.")
            df_gold = pd.merge(df_gold, df_new, how='outer', on='google_id' if 'google' in entity else 'usdot_num', suffixes=('_gold', '_silver'))

            for col in df_new.columns:
                if col != 'google_id' and col != 'usdot_num':
                    df_gold[col] = df_gold[f'{col}_silver'].combine_first(df_gold[f'{col}_gold'])
                    df_gold.drop(columns=[f'{col}_gold', f'{col}_silver'], inplace=True)
            
            # Count duplicates before removing
            duplicates_count = df_gold.duplicated(subset=['google_id' if 'google' in entity else 'usdot_num'], keep='last').sum()
            print(f"Number of duplicate rows to be removed: {duplicates_count}")
            
            # Drop duplicates
            df_gold.drop_duplicates(subset=['google_id' if 'google' in entity else 'usdot_num'], keep='last', inplace=True)
        else:
            print(f"No update needed. SILVER data is older or equal to GOLD's last update.")
    else:
        print("GOLD file does not exist. Inserting new data.")
        df_gold = df_new
    
    print(f'df_gold: {df_gold.shape}')
    df_gold.to_parquet(full_gold_file, index=False)
