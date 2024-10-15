import pandas as pd
from .data_loading import load_parquet_data

# Function to clean suffixes from columns
def clean_column_suffixes(df):
    """
    Removes '_x' suffixes from column names and drops columns with '_y' suffixes.
    
    Args:
    df (pandas.DataFrame): The DataFrame with potential '_x' and '_y' suffixed columns.
    
    Returns:
    pandas.DataFrame: DataFrame with '_x' suffixes removed and '_y' columns dropped.
    """
    df.columns = df.columns.str.replace('_x$', '', regex=True)
    df = df.loc[:, ~df.columns.str.endswith('_y')]
    return df

# Function to join company profiles and customer reviews
def join_company_profiles_and_reviews(silver_path):
    """
    Performs a JOIN between company profiles and customer reviews based on 'google_id'.
    
    Args:
    silver_path (str): Base path where the entities are located (e.g., './SILVER/').
    
    Returns:
    pandas.DataFrame: DataFrame resulting from the JOIN operation for Google-related data.
    """
    df_profiles = load_parquet_data(silver_path, 'company_profiles_google_maps')
    print(f'df_profiles: {df_profiles.shape}' )
    df_reviews = load_parquet_data(silver_path, 'customer_reviews_google')
    print(f'df_reviews: {df_reviews.shape}' )
    df_joined_google = pd.merge(df_profiles, df_reviews, on='google_id', how='left')
    print(f'df_joined_google: {df_joined_google.shape}' )
    # Clean '_x' and '_y' suffixes from columns
    df_joined_google = clean_column_suffixes(df_joined_google)
    print(f'df_joined_google_suf: {df_joined_google.shape}' )
    
    return df_joined_google

# Function to join FMCSA data
def join_fmcsa_data(silver_path):
    """
    Performs a JOIN between FMCSA companies, snapshots, and complaints based on 'usdot_num'.
    
    Args:
    silver_path (str): Base path where the entities are located (e.g., './SILVER/').
    
    Returns:
    pandas.DataFrame: DataFrame resulting from the JOIN operation for FMCSA-related data.
    """
    df_companies = load_parquet_data(silver_path, 'fmcsa_companies')
    df_snapshot = load_parquet_data(silver_path, 'fmcsa_company_snapshot')
    df_complaints = load_parquet_data(silver_path, 'fmcsa_complaints')
    df_safer = load_parquet_data(silver_path, 'fmcsa_safer_data')
    
    # Perform join and clean suffixes
    df_joined_fmcsa = pd.merge(df_companies, df_snapshot, on='usdot_num', how='left')
    df_joined_fmcsa = clean_column_suffixes(df_joined_fmcsa)
    
    df_joined_fmcsa = pd.merge(df_joined_fmcsa, df_complaints, on='usdot_num', how='left')
    df_joined_fmcsa = clean_column_suffixes(df_joined_fmcsa)
    
    df_joined_fmcsa = pd.merge(df_joined_fmcsa, df_safer, on='usdot_num', how='left')
    df_joined_fmcsa = clean_column_suffixes(df_joined_fmcsa)
    
    return df_joined_fmcsa
