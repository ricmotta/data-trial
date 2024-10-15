import pandas as pd

# Function to apply data transformations specific to each entity
def transform_data(df, entity):
    """
    Applies necessary data transformations specific to each entity.

    :param df: DataFrame to be transformed
    :param entity: The entity (dataset) name to apply specific transformations
    :return: Transformed DataFrame
    """
    if entity == "company_profiles_google_maps":
        df['postal_code'] = df['postal_code'].str.replace('.0', '', regex=False)
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce').astype(float)
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce').astype(float)
        df['area_service'] = df['area_service'].astype(bool)
        df['rating'] = pd.to_numeric(df['rating'], errors='coerce').astype(float)
        df['photos_count'] = pd.to_numeric(df['photos_count'], errors='coerce').astype('Int64')
    
    elif entity == "customer_reviews_google":
        df['reviews'] = pd.to_numeric(df['reviews'], errors='coerce').astype('Int64')
        df['rating'] = pd.to_numeric(df['rating'], errors='coerce').astype(float)
        df['author_reviews_count'] = pd.to_numeric(df['author_reviews_count'], errors='coerce').astype('Int64')
        df['owner_answer_timestamp_datetime_utc'] = pd.to_datetime(df['owner_answer_timestamp_datetime_utc'], errors='coerce')
        df['review_rating'] = pd.to_numeric(df['review_rating'], errors='coerce').astype('Int64')
        df['review_datetime_utc'] = pd.to_datetime(df['review_datetime_utc'], errors='coerce')
        df['review_likes'] = pd.to_numeric(df['review_likes'], errors='coerce').astype('Int64')

    elif entity == "fmcsa_companies":
        df['usdot_num'] = pd.to_numeric(df['usdot_num'], errors='coerce').astype('Int64')
        df['date_created'] = pd.to_datetime(df['date_created'], errors='coerce')
        df['date_updated'] = pd.to_datetime(df['date_updated'], errors='coerce')
        df['total_complaints_2021'] = pd.to_numeric(df['total_complaints_2021'], errors='coerce').astype('Int64')
        df['total_complaints_2022'] = pd.to_numeric(df['total_complaints_2022'], errors='coerce').astype('Int64')
        df['total_complaints_2023'] = pd.to_numeric(df['total_complaints_2023'], errors='coerce').astype('Int64')
    
    elif entity == "fmcsa_company_snapshot":
        df['usdot_num'] = pd.to_numeric(df['usdot_num'], errors='coerce').astype('Int64')
        df['date_created'] = pd.to_datetime(df['date_created'], errors='coerce')
        df['date_updated'] = pd.to_datetime(df['date_updated'], errors='coerce')
        df['mc_num'] = pd.to_numeric(df['mc_num'], errors='coerce').astype('Int64')
        df['num_of_trucks'] = pd.to_numeric(df['num_of_trucks'], errors='coerce').astype('Int64')
        df['num_of_tractors'] = pd.to_numeric(df['num_of_tractors'], errors='coerce').astype('Int64')
        df['num_of_trailers'] = pd.to_numeric(df['num_of_trailers'], errors='coerce').astype('Int64')
        df['hhg_authorization'] = df['hhg_authorization'].astype(bool)
        df['total_complaints_2021'] = pd.to_numeric(df['total_complaints_2021'], errors='coerce').astype('Int64')
        df['total_complaints_2022'] = pd.to_numeric(df['total_complaints_2022'], errors='coerce').astype('Int64')
        df['total_complaints_2023'] = pd.to_numeric(df['total_complaints_2023'], errors='coerce').astype('Int64')
    
    elif entity == "fmcsa_complaints":
        df['usdot_num'] = pd.to_numeric(df['usdot_num'], errors='coerce').astype('Int64')
        df['date_created'] = pd.to_datetime(df['date_created'], errors='coerce')
        if 'date_updated' in df.columns:
            df['date_updated'] = pd.to_datetime(df['date_updated'], errors='coerce')
        df['complaint_count'] = pd.to_numeric(df['complaint_count'], errors='coerce').astype('Int64')

    elif entity == "fmcsa_safer_data":
        df['usdot_num'] = pd.to_numeric(df['usdot_num'], errors='coerce').astype('Int64')
        df['date_created'] = pd.to_datetime(df['date_created'], errors='coerce')
        df['date_updated'] = pd.to_datetime(df['date_updated'], errors='coerce')
        df['oos_date'] = pd.to_datetime(df['oos_date'], errors='coerce').dt.date
        df['power_units'] = pd.to_numeric(df['power_units'], errors='coerce').astype('Int64')
        df['drivers'] = pd.to_numeric(df['drivers'], errors='coerce').astype('Int64')
        df['mcs_150_form_date'] = pd.to_datetime(df['mcs_150_form_date'], errors='coerce').dt.date
        df['us_vehicle_inspections'] = pd.to_numeric(df['us_vehicle_inspections'], errors='coerce').astype('Int64')
        df['us_driver_inspections'] = pd.to_numeric(df['us_driver_inspections'], errors='coerce').astype('Int64')
        df['us_hazmat_inspections'] = pd.to_numeric(df['us_hazmat_inspections'], errors='coerce').astype('Int64')
        df['us_iep_inspections'] = pd.to_numeric(df['us_iep_inspections'], errors='coerce').astype('Int64')
        df['us_vehicle_out_of_service'] = pd.to_numeric(df['us_vehicle_out_of_service'], errors='coerce').astype('Int64')
        df['us_driver_out_of_service'] = pd.to_numeric(df['us_driver_out_of_service'], errors='coerce').astype('Int64')
        df['us_hazmat_out_of_service'] = pd.to_numeric(df['us_hazmat_out_of_service'], errors='coerce').astype('Int64')
        df['us_iep_out_of_service'] = pd.to_numeric(df['us_iep_out_of_service'], errors='coerce').astype('Int64')
        df['us_vehicle_out_of_service_pct'] = pd.to_numeric(df['us_vehicle_out_of_service_pct'], errors='coerce').astype(float)
        df['us_driver_out_of_service_pct'] = pd.to_numeric(df['us_driver_out_of_service_pct'], errors='coerce').astype(float)
        df['us_hazmat_out_of_service_pct'] = pd.to_numeric(df['us_hazmat_out_of_service_pct'], errors='coerce').astype(float)
        df['us_iep_out_of_service_pct'] = pd.to_numeric(df['us_iep_out_of_service_pct'], errors='coerce').astype(float)
        df['us_vehicle_natl_avg_oos_pct'] = pd.to_numeric(df['us_vehicle_natl_avg_oos_pct'], errors='coerce').astype(float)
        df['us_driver_natl_avg_oos_pct'] = pd.to_numeric(df['us_driver_natl_avg_oos_pct'], errors='coerce').astype(float)
        df['us_hazmat_natl_avg_oos_pct'] = pd.to_numeric(df['us_hazmat_natl_avg_oos_pct'], errors='coerce').astype(float)
        if 'us_iep_natl_avg_oos_pct' in df.columns:
            df['us_iep_natl_avg_oos_pct'] = pd.to_numeric(df['us_iep_natl_avg_oos_pct'], errors='coerce').astype(float)
        df['us_crashes_fatal'] = pd.to_numeric(df['us_crashes_fatal'], errors='coerce').astype('Int64')
        df['us_crashes_injury'] = pd.to_numeric(df['us_crashes_injury'], errors='coerce').astype('Int64')
        df['us_crashes_tow'] = pd.to_numeric(df['us_crashes_tow'], errors='coerce').astype('Int64')
        df['us_crashes_total'] = pd.to_numeric(df['us_crashes_total'], errors='coerce').astype('Int64')
        df['canadian_vehicle_inspections'] = pd.to_numeric(df['canadian_vehicle_inspections'], errors='coerce').astype('Int64')
        df['canadian_driver_inspections'] = pd.to_numeric(df['canadian_driver_inspections'], errors='coerce').astype('Int64')
        df['canadian_vehicle_out_of_service'] = pd.to_numeric(df['canadian_vehicle_out_of_service'], errors='coerce').astype('Int64')
        df['canadian_driver_out_of_service'] = pd.to_numeric(df['canadian_driver_out_of_service'], errors='coerce').astype('Int64')
        df['canadian_vehicle_out_of_service_pct'] = pd.to_numeric(df['canadian_vehicle_out_of_service_pct'], errors='coerce').astype(float)
        df['canadian_driver_out_of_service_pct'] = pd.to_numeric(df['canadian_driver_out_of_service_pct'], errors='coerce').astype(float)
        df['carrier_safety_rating_rating_date'] = pd.to_datetime(df['carrier_safety_rating_rating_date'], errors='coerce').dt.date
        df['carrier_safety_rating_review_date'] = pd.to_datetime(df['carrier_safety_rating_review_date'], errors='coerce').dt.date
        df['mileage'] = pd.to_numeric(df['mileage'], errors='coerce').astype('Int64')

    return df
