from scripts.database_operations.utils.parquet_processing import process_parquet_to_postgres
from scripts.database_operations.utils.db_operations import engine

# Example usage with two parquet files from the GOLD layer
def main():
    process_parquet_to_postgres('./data/GOLD/real_estate_agents_classified/real_estate_agents_classified.parquet', 'google_agents_data', engine)
    process_parquet_to_postgres('./data/GOLD/fmcsa_company_data/fmcsa_company_data.parquet', 'fmcsa_data', engine)

if __name__ == "__main__":
    main()
