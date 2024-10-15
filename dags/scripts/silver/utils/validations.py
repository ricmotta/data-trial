import great_expectations as gx

def validate_data(df, entity):
    """
    Validates the data using Great Expectations based on the inferred data types of the DataFrame
    and specific column uniqueness rules for certain entities.

    :param df: DataFrame to be validated
    :param entity: The entity name to apply specific validations
    :return: Validation result
    """
    # Create the Data Context
    context = gx.get_context()

    # Set up the data source and batch
    data_source = context.data_sources.add_pandas("pandas")
    data_asset = data_source.add_dataframe_asset(name="pd_dataframe_asset")
    
    batch_definition = data_asset.add_batch_definition_whole_dataframe("batch_definition")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    # Create an Expectation Suite
    suite_name = "my_expectation_suite"
    suite = gx.ExpectationSuite(name=suite_name)
    suite = context.suites.add(suite)

    # Map pandas data types to Great Expectations types
    type_mapping = {
        'object': 'str',
        'int64': 'int',
        'float64': 'float',
        'bool': 'boolean',
        'datetime64[ns]': 'datetime',
        'Int64': 'int',  # for nullable integer type
        'Float64': 'float'  # for nullable float type
    }

    # Generate expectations for each column based on inferred data type
    print(f"Validating column data types in entity: {entity}")
    for column in df.columns:
        pandas_type = df[column].dtype
        expected_type = type_mapping.get(str(pandas_type), 'string')
        expectation = gx.expectations.ExpectColumnValuesToBeOfType(column=column, type_=expected_type)
        suite.add_expectation(expectation)

    # Entity-specific uniqueness validation
    if entity in ['company_profiles_google_maps', 'customer_reviews_google']:
        # Expect 'google_id' to have unique values
        if 'google_id' in df.columns:
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="google_id"))
            print(f"Validating uniqueness for 'google_id' in entity: {entity}")

    elif entity in ['fmcsa_companies', 'fmcsa_company_snapshot', 'fmcsa_complaints', 'fmcsa_safer_data']:
        # Expect 'usdot_num' to have unique values
        if 'usdot_num' in df.columns:
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="usdot_num"))
            print(f"Validating uniqueness for 'usdot_num' in entity: {entity}")

    # Validate the batch with the expectation suite
    validation_result = batch.validate(suite)

    return validation_result