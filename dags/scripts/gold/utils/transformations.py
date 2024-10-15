from sklearn.preprocessing import MinMaxScaler
import pandas as pd

# Function to classify real estate agents/agencies
def classify_real_estate_agents(df):
    """
    Classifies real estate agents/agencies based on rating, number of reviews, and photos count.
    
    Args:
    df (pandas.DataFrame): The DataFrame containing real estate agent/agencies data.
    
    Returns:
    pandas.DataFrame: The DataFrame with an additional 'performance_score' column.
    """
    df_filtered = df.dropna(subset=['rating', 'reviews', 'photos_count']).copy()
    
    scaler = MinMaxScaler()
    df_filtered['rating_scaled'] = scaler.fit_transform(df_filtered[['rating']])
    df_filtered['reviews_scaled'] = scaler.fit_transform(df_filtered[['reviews']])
    df_filtered['photos_scaled'] = scaler.fit_transform(df_filtered[['photos_count']])
    
    df_filtered['performance_score'] = (
        df_filtered['rating_scaled'] * 0.5 +  
        df_filtered['reviews_scaled'] * 0.4 +  
        df_filtered['photos_scaled'] * 0.1  
    )
    
    df_filtered = df_filtered.sort_values(by='performance_score', ascending=False)
    
    return df_filtered
