import os
import shutil
from datetime import datetime

def save_raw_files_to_bronze(source_dir, bronze_dir):
    """
    Moves or copies raw files to the bronze layer, organized by date.

    :param source_dir: Directory where the raw CSV files are located
    :param bronze_dir: Destination directory in the bronze layer
    """
    # Get the current date to structure the directories
    date_str = datetime.now().strftime('%Y-%m-%d')
    
    # List all files in the source directory
    for file_name in os.listdir(source_dir):
        # Check if the file is a CSV
        if file_name.endswith('.csv'):
            # Define the entity name based on the file name
            entity_name = file_name.split('.')[0]
            
            # Define the destination directory in the bronze layer
            destination_dir = os.path.join(bronze_dir, entity_name, date_str)
            
            # Create the directory if it does not exist
            os.makedirs(destination_dir, exist_ok=True)
            
            # Full paths for the source and destination files
            source_file = os.path.join(source_dir, file_name)
            destination_file = os.path.join(destination_dir, file_name)
            
            # Copy the file to the bronze layer
            shutil.copy2(source_file, destination_file)
            print(f'File {file_name} copied to {destination_file}')
