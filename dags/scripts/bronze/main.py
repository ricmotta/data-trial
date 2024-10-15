from scripts.bronze.utils.file_manager import save_raw_files_to_bronze

def execute_bronze_layer():
    """
    Executes the process of saving raw files to the bronze layer.
    """
    source_directory = "./dags/scripts/data_examples"
    bronze_directory = "./data/BRONZE"
    
    save_raw_files_to_bronze(source_directory, bronze_directory)

if __name__ == "__main__":
    execute_bronze_layer()