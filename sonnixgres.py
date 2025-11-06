def create_table(connection, table_name):
    """Create a new table if it does not exist."""
    # SECURITY FIX: Sanitize table name to prevent SQL injection
    sanitized_table = sanitize_sql_identifier(table_name)

    try:
        cursor = connection.cursor()
        create_table_query = f"CREATE TABLE IF NOT EXISTS {AsIs(sanitized_table)} ();"
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        logger.info(f"Table '{sanitized_table}' created successfully.")
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error in creating table: {error}")
        raise

def populate_table(connection, table_name, dataframe):
    """Populate the table with data from a DataFrame."""
    # SECURITY FIX: Sanitize table name and column names to prevent SQL injection
    sanitized_table = sanitize_sql_identifier(table_name)
    sanitized_columns = [sanitize_sql_identifier(col) for col in dataframe.columns]

    try:
        cursor = connection.cursor()

        # Add columns based on DataFrame, one at a time
        for col in sanitized_columns:
            alter_table_query = f"ALTER TABLE {AsIs(sanitized_table)} ADD COLUMN IF NOT EXISTS {AsIs(col)} TEXT;"
            cursor.execute(alter_table_query)

        # Insert data
        insert_columns = ', '.join(sanitized_columns)
        insert_values = ', '.join(['%s'] * len(sanitized_columns))
        insert_query = f"INSERT INTO {AsIs(sanitized_table)} ({insert_columns}) VALUES ({insert_values})"
        cursor.executemany(insert_query, dataframe.values.tolist())
        connection.commit()
        cursor.close()
        logger.info(f"Data inserted into table '{sanitized_table}' successfully.")
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error in populating table: {error}")
        raise

def update_records(connection: psycopg2.connect, update_query: str,
                   params: tuple | None = None, close_connection: bool = True) -> None:
    if not connection:
        raise ConnectionError("No connection to database.")

    try:
        with connection.cursor() as cursor:
            cursor.execute(update_query, params)
            connection.commit()
            logger.info("Update query executed successfully.")
    except (Exception, psycopg2.DatabaseError) as error:
        connection.rollback()
        logger.error(f"Update query execution error: {error}")
        raise
    finally:
        if close_connection:
            connection.close()
            logger.info("Database connection closed.")

def create_view(connection: psycopg2.connect, view_name: str, view_query: str,
                close_connection: bool = True) -> None:
    if not connection:
        raise ConnectionError("No database connection provided.")

    # SECURITY FIX: Sanitize view name to prevent SQL injection
    sanitized_view = sanitize_sql_identifier(view_name)

    try:
        with connection.cursor() as cursor:
            create_view_query = f"CREATE OR REPLACE VIEW {AsIs(sanitized_view)} AS {view_query}"
            cursor.execute(create_view_query)
            connection.commit()
            logger.info(f"View '{sanitized_view}' created successfully.")
    except (Exception, psycopg2.DatabaseError) as error:
        connection.rollback()
        logger.error(f"Error creating view '{sanitized_view}': {error}")
        raise
    finally:
        if close_connection:
            connection.close()
            logger.info("Database connection closed.")