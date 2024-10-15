import os
import pandas as pd
from datetime import datetime
from validations import validate_data
from transformations import transform_data

# Function to identify the maximum number of columns in a CSV file
def find_max_columns(csv_file):
    with open(csv_file, 'r', encoding='utf-8') as file:
        max_columns = max(len(line.split(',')) for line in file)
    return max_columns

# Function to delete columns with all null values
def delete_null_columns(df):
    """
    Deletes columns where all values are null.
    :param df: DataFrame before cleaning
    :return: Cleaned DataFrame
    """
    non_null_counts = df.notna().sum()
    columns_to_delete = non_null_counts[non_null_counts == 0].index.tolist()
    print("Columns deleted (all rows null):", columns_to_delete)
    df_cleaned = df.drop(columns=columns_to_delete)
    print(f"Total columns deleted: {len(columns_to_delete)}")
    return df_cleaned

# Function to process the CSV file
def process_csv(csv_file):
    """
    Reads a CSV file, processes it by dynamically generating column names and removing null values.
    :param csv_file: Path to the CSV file
    :return: Processed DataFrame
    """
    max_columns = find_max_columns(csv_file)
    column_names = [i for i in range(max_columns)]
    
    df = pd.read_csv(csv_file, header=None, names=column_names, engine='python', quotechar='"', quoting=1, escapechar='\\')
    
    header_line = df.iloc[0]
    header_line = header_line[header_line.notna()]
    df = df[header_line.index]
    df = df.drop(index=0)
    df.columns = header_line
    
    df_cleaned = delete_null_columns(df)
    rows_before_cleaning = len(df_cleaned)
    df_cleaned = df_cleaned.dropna(how='all').drop_duplicates()
    rows_removed = rows_before_cleaning - len(df_cleaned)
    print(f"Total empty or duplicate rows removed: {rows_removed}")
    
    return df_cleaned

# Function to perform UPSERT/MERGE based on updated_at
def upsert_to_silver(silver_file, df_new, entity, bronze_file_mtime):
    """
    Performs UPSERT/MERGE into the SILVER layer based on the modification time of the files.
    If updated_at doesn't exist, it inserts the data. If it exists, only updates if BRONZE data is newer.
    :param silver_file: Path to the SILVER parquet file
    :param df_new: DataFrame from BRONZE
    :param entity: The entity name to determine the primary key column
    :param bronze_file_mtime: Modification time of the BRONZE file
    """
    # Determine the primary key based on the entity
    primary_key = 'google_id' if entity in ['company_profiles_google_maps', 'customer_reviews_google'] else 'usdot_num'
    
    # Ensure that the directory for the SILVER file exists
    os.makedirs(os.path.dirname(silver_file), exist_ok=True)
    
    if os.path.exists(silver_file):
        # Load existing SILVER data
        df_silver = pd.read_parquet(silver_file)
        
        # Get the modification time of the SILVER file
        silver_file_mtime = os.path.getmtime(silver_file)

        # Print the comparison of the BRONZE and SILVER update times
        print(f"Comparing BRONZE modified at {datetime.fromtimestamp(bronze_file_mtime)} with SILVER last updated at {datetime.fromtimestamp(silver_file_mtime)}")

        if bronze_file_mtime > silver_file_mtime:
            print(f"BRONZE data is newer. Performing UPSERT.")
            # Perform the UPSERT logic: merge new data into SILVER, updating or inserting
            df_silver = pd.merge(df_silver, df_new, how='outer', on=primary_key, suffixes=('_silver', '_new'))
            
            # Combine or update columns
            for col in df_new.columns:
                if col != primary_key:
                    df_silver[col] = df_silver[f'{col}_new'].combine_first(df_silver[f'{col}_silver'])
                    df_silver.drop(columns=[f'{col}_silver', f'{col}_new'], inplace=True)
            
            df_silver.drop_duplicates(subset=primary_key, keep='last', inplace=True)
        else:
            print(f"No update needed. BRONZE data is older or equal to SILVER's last update.")
    else:
        # SILVER file does not exist, insert all new data
        print("SILVER file does not exist. Inserting new data.")
        df_silver = df_new
    
    # Save the updated data back to the SILVER layer
    df_silver.to_parquet(silver_file, index=False)

# Function to process only the most recent CSV from the BRONZE layer based on file modification time and PATH {date}
def process_bronze_to_silver(bronze_dir, silver_dir):
    """
    Processes the most recent CSV file from the BRONZE layer based on file modification time and {date} in the PATH,
    applies transformations, and saves the results to the SILVER layer.
    :param bronze_dir: Path to the BRONZE directory
    :param silver_dir: Path to the SILVER directory
    """
    for entity in os.listdir(bronze_dir):
        print(f'Performing processing for entity: {entity}')
        entity_path = os.path.join(bronze_dir, entity)
        if os.path.isdir(entity_path):
            # Find the most recent date in the BRONZE layer based on the folder structure
            recent_date = max(os.listdir(entity_path), key=lambda d: datetime.strptime(d, '%Y-%m-%d'))
            date_path = os.path.join(entity_path, recent_date)
            
            if os.path.isdir(date_path):
                # Process the most recent file based on modification time
                most_recent_file = max([os.path.join(date_path, f) for f in os.listdir(date_path) if f.endswith('.csv')],
                                       key=os.path.getmtime)
                
                # Get the modification time of the BRONZE file
                bronze_file_mtime = os.path.getmtime(most_recent_file)
                
                df_cleaned = process_csv(most_recent_file)
                df_transformed = transform_data(df_cleaned, entity)
                validation_result = validate_data(df_transformed, entity)
                
                if validation_result:
                    silver_file = os.path.join(silver_dir, entity, os.path.basename(most_recent_file).replace('.csv', '.parquet'))
                    upsert_to_silver(silver_file, df_transformed, entity, bronze_file_mtime)

# Main function to orchestrate the process
def main():
    bronze_dir = "./data/BRONZE"
    silver_dir = "./data/SILVER"
    process_bronze_to_silver(bronze_dir, silver_dir)

if __name__ == "__main__":
    main()