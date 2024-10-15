import os
from scripts.gold.utils.joins import join_company_profiles_and_reviews, join_fmcsa_data
from scripts.gold.utils.transformations import classify_real_estate_agents
from scripts.gold.utils.upsert import upsert_to_gold

# Function to handle the transformation and UPSERT for the GOLD layer
def process_silver_to_gold(silver_dir, gold_dir):
    """
    Processes data from the SILVER layer, applies transformations, performs UPSERT to the GOLD layer.
    
    Args:
    silver_dir (str): Path to the SILVER directory
    gold_dir (str): Path to the GOLD directory
    """
    df_joined_google = join_company_profiles_and_reviews(silver_dir)
    print(f'df_joined_na_main: {df_joined_google.shape}')
    # df_google_classified_agents = classify_real_estate_agents(df_joined_google)
    # print(f'df_classified: {df_google_classified_agents.shape}')
    
    df_joined_fmcsa = join_fmcsa_data(silver_dir)

    # Save classified agents and FMCSA data to GOLD layer with modified path
    upsert_to_gold(
        os.path.join(gold_dir, 'real_estate_agents_classified.parquet'),
        df_joined_google,
        'real_estate_agents_classified',
        os.path.getmtime(silver_dir)
    )

    upsert_to_gold(
        os.path.join(gold_dir, 'fmcsa_company_data.parquet'),
        df_joined_fmcsa,
        'fmcsa_company_data',
        os.path.getmtime(silver_dir)
    )

# Example usage to orchestrate the process
def main():
    silver_dir = "./data/SILVER"
    gold_dir = "./data/GOLD"
    process_silver_to_gold(silver_dir, gold_dir)

if __name__ == "__main__":
    main()
