def populate_table(connection: connection, table_name: str, dataframe: pd.DataFrame, batch_size: int = 1000) -> None:
    if dataframe.empty:
        raise ValueError("DataFrame cannot be empty")

    if batch_size <= 0:
        raise ValueError("Batch size must be positive")

    sanitized_table = sanitize_sql_identifier(table_name)
    total_rows = len(dataframe)

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

            data_values = dataframe.values.tolist()
            inserted_rows = 0

            # Use batching for large datasets
            for i in range(0, len(data_values), batch_size):
                batch = data_values[i:i + batch_size]
                cursor.executemany(insert_query, batch)
                inserted_rows += len(batch)

                # Log progress for large inserts
                if total_rows > 10000 and (i + batch_size) % 10000 == 0:
                    progress = min(i + batch_size, total_rows)
                    logger.info(f"Inserted {progress}/{total_rows} rows ({progress/total_rows*100:.1f}%)")

            connection.commit()

        logger.info(f"Successfully inserted {total_rows} rows into table '{sanitized_table}' using {len(columns)} columns")

    except psycopg2.DatabaseError as e:
        connection.rollback()
        logger.error(f"Failed to populate table '{sanitized_table}': {e}")
        raise