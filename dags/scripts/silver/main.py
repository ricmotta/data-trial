import os
from scripts.silver.utils.file_operations import process_csv, upsert_to_silver
from scripts.silver.utils.transformations import transform_data
# from scripts.silver.utils.validations import validate_data
from datetime import datetime

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
                # validation_result = validate_data(df_transformed, entity)
                
                #if validation_result:
                silver_file = os.path.join(silver_dir, entity, os.path.basename(most_recent_file).replace('.csv', '.parquet'))
                upsert_to_silver(silver_file, df_transformed, entity, bronze_file_mtime)

# Main function to orchestrate the process
def main():
    bronze_dir = "./data/BRONZE"
    silver_dir = "./data/SILVER"
    process_bronze_to_silver(bronze_dir, silver_dir)

if __name__ == "__main__":
    main()
