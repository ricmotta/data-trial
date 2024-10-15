def infer_sql_schema_from_parquet(df, table_name):
    """
    Infers the SQL schema from a DataFrame to create a table in PostgreSQL.
    Adds an 'updated_at' column to track data updates.
    """
    print(f"Inferring SQL schema for table: {table_name}")
    
    schema = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
    
    # Mapping pandas data types to SQL data types
    type_mapping = {
        'object': 'TEXT',
        'int64': 'INTEGER',
        'float64': 'DECIMAL',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP',
        'timedelta[ns]': 'INTERVAL',
        'Int64': 'INTEGER',
        'Float64': 'DECIMAL',
        'datetime64[ns, UTC]': 'TIMESTAMP'
    }
    
    for col in df.columns:
        dtype = str(df[col].dtype)
        sql_type = type_mapping.get(dtype, 'TEXT')  # Default to TEXT if type is not mapped
        schema += f"    {col} {sql_type},\n"
    
    # Add 'updated_at' column
    schema += "    updated_at TIMESTAMP\n"
    
    schema = schema.rstrip(',\n')  # Remove the last comma
    schema += "\n);"
    
    print(f"SQL schema inferred: \n{schema}")
    return schema
