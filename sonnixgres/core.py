def populate_table(connection: connection, table_name: str, dataframe: pd.DataFrame) -> None:
    if dataframe.empty:
        raise ValueError("DataFrame cannot be empty")

    sanitized_table = sanitize_sql_identifier(table_name)

    try:
        with connection.cursor() as cursor:
            # Infer appropriate PostgreSQL types for each column
            for col in dataframe.columns:
                sanitized_col = sanitize_sql_identifier(col)
                # Get sample values for smarter type inference
                sample_values = dataframe[col].dropna().head(10).tolist() if len(dataframe) > 0 else None
                postgres_type = _infer_postgresql_type(str(dataframe[col].dtype), sample_values)

                alter_query = "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s"
                cursor.execute(alter_query, (AsIs(sanitized_table), AsIs(sanitized_col), postgres_type))

            columns = [sanitize_sql_identifier(col) for col in dataframe.columns]
            insert_columns = ', '.join(columns)
            placeholders = ', '.join(['%s'] * len(columns))
            insert_query = f"INSERT INTO {AsIs(sanitized_table)} ({insert_columns}) VALUES ({placeholders})"

            batch_size = 1000
            data_values = dataframe.values.tolist()

            for i in range(0, len(data_values), batch_size):
                batch = data_values[i:i + batch_size]
                cursor.executemany(insert_query, batch)

            connection.commit()

        logger.info(f"Inserted {len(dataframe)} rows into table '{sanitized_table}' with inferred column types")

    except psycopg2.DatabaseError as e:
        connection.rollback()
        logger.error(f"Failed to populate table '{sanitized_table}': {e}")
        raise