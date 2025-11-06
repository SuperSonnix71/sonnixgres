"""
Sonnixgres - High-performance PostgreSQL operations with robust error handling.
"""

import time
from typing import Optional
from contextlib import contextmanager
import psycopg2
from psycopg2.extensions import AsIs
import pandas as pd

# Import custom modules
from sonnixgres.exceptions import (
    SonnixgresError,
    ConnectionError,
    QueryError,
    TableError,
    ValidationError,
    TransactionError
)
from sonnixgres.logging_config import logger, log_error, log_performance
from sonnixgres.validation import (
    validate_table_name,
    validate_dataframe,
    validate_query_params,
    validate_view_query,
    sanitize_sql_identifier
)


def create_table(connection, table_name):
    """Create a new table if it does not exist."""
    start_time = time.time()

    # Validate inputs
    validate_table_name(table_name)

    # SECURITY FIX: Sanitize table name to prevent SQL injection
    sanitized_table = sanitize_sql_identifier(table_name)

    try:
        with connection.cursor() as cursor:
            create_table_query = f"CREATE TABLE IF NOT EXISTS {AsIs(sanitized_table)} ();"
            cursor.execute(create_table_query)
            connection.commit()
            logger.info(f"Table '{sanitized_table}' created successfully.")

        log_performance("create_table", time.time() - start_time, table_name=sanitized_table)

    except psycopg2.errors.DuplicateTable:
        # Table already exists - this is not an error
        logger.warning(f"Table '{sanitized_table}' already exists.")
    except psycopg2.OperationalError as error:
        connection.rollback()
        log_error(error, "create_table", table_name=sanitized_table)
        raise ConnectionError(f"Connection error while creating table '{table_name}': {error}") from error
    except psycopg2.ProgrammingError as error:
        connection.rollback()
        log_error(error, "create_table", table_name=sanitized_table)
        raise QueryError(f"SQL syntax error while creating table '{table_name}': {error}") from error
    except Exception as error:
        connection.rollback()
        log_error(error, "create_table", table_name=sanitized_table)
        raise SonnixgresError(f"Unexpected error while creating table '{table_name}': {error}") from error


def populate_table(connection, table_name, dataframe):
    """Populate the table with data from a DataFrame."""
    start_time = time.time()

    # Validate inputs
    validate_table_name(table_name)
    validate_dataframe(dataframe, "populate_table")

    # SECURITY FIX: Sanitize table name and column names to prevent SQL injection
    sanitized_table = sanitize_sql_identifier(table_name)
    sanitized_columns = [sanitize_sql_identifier(col) for col in dataframe.columns]

    try:
        with connection.cursor() as cursor:
            # Add columns based on DataFrame, one at a time
            for col in sanitized_columns:
                alter_table_query = f"ALTER TABLE {AsIs(sanitized_table)} ADD COLUMN IF NOT EXISTS {AsIs(col)} TEXT;"
                cursor.execute(alter_table_query)

            # Insert data in batches for better performance
            batch_size = 1000
            total_rows = len(dataframe)

            for i in range(0, total_rows, batch_size):
                batch_df = dataframe.iloc[i:i+batch_size]
                insert_columns = ', '.join(sanitized_columns)
                insert_values = ', '.join(['%s'] * len(sanitized_columns))
                insert_query = f"INSERT INTO {AsIs(sanitized_table)} ({insert_columns}) VALUES ({insert_values})"
                cursor.executemany(insert_query, batch_df.values.tolist())

            connection.commit()
            logger.info(f"Data inserted into table '{sanitized_table}' successfully ({total_rows} rows).")

        log_performance("populate_table", time.time() - start_time,
                       table_name=sanitized_table, rows_inserted=total_rows)

    except psycopg2.errors.UndefinedTable as error:
        connection.rollback()
        log_error(error, "populate_table", table_name=sanitized_table)
        raise TableError(f"Table '{table_name}' does not exist") from error
    except psycopg2.OperationalError as error:
        connection.rollback()
        log_error(error, "populate_table", table_name=sanitized_table)
        raise ConnectionError(f"Connection error while populating table '{table_name}': {error}") from error
    except psycopg2.DataError as error:
        connection.rollback()
        log_error(error, "populate_table", table_name=sanitized_table)
        raise QueryError(f"Data error while populating table '{table_name}': {error}") from error
    except Exception as error:
        connection.rollback()
        log_error(error, "populate_table", table_name=sanitized_table)
        raise SonnixgresError(f"Unexpected error while populating table '{table_name}': {error}") from error


def update_records(connection, update_query: str,
                   params: Optional[tuple] = None, close_connection: bool = True) -> None:
    start_time = time.time()

    # Validate inputs
    if not connection:
        raise ConnectionError("No connection to database.")
    validate_query_params(update_query, params)

    try:
        with connection.cursor() as cursor:
            cursor.execute(update_query, params)
            connection.commit()
            logger.info("Update query executed successfully.")

        log_performance("update_records", time.time() - start_time)

    except psycopg2.errors.UndefinedTable as error:
        connection.rollback()
        log_error(error, "update_records")
        raise TableError(f"Table referenced in update query does not exist: {error}") from error
    except psycopg2.ProgrammingError as error:
        connection.rollback()
        log_error(error, "update_records")
        raise QueryError(f"SQL syntax error in update query: {error}") from error
    except psycopg2.OperationalError as error:
        connection.rollback()
        log_error(error, "update_records")
        raise ConnectionError(f"Connection error during update: {error}") from error
    except Exception as error:
        connection.rollback()
        log_error(error, "update_records")
        raise TransactionError(f"Transaction failed during update: {error}") from error
    finally:
        if close_connection:
            try:
                connection.close()
                logger.debug("Database connection closed.")
            except Exception as close_error:
                logger.warning(f"Error closing connection: {close_error}")


def create_view(connection, view_name: str, view_query: str,
                close_connection: bool = True) -> None:
    start_time = time.time()

    # Validate inputs
    if not connection:
        raise ConnectionError("No database connection provided.")
    validate_table_name(view_name)  # Views use same naming rules as tables
    validate_view_query(view_query)

    # SECURITY FIX: Sanitize view name to prevent SQL injection
    sanitized_view = sanitize_sql_identifier(view_name)

    try:
        with connection.cursor() as cursor:
            create_view_query = f"CREATE OR REPLACE VIEW {AsIs(sanitized_view)} AS {view_query}"
            cursor.execute(create_view_query)
            connection.commit()
            logger.info(f"View '{sanitized_view}' created successfully.")

        log_performance("create_view", time.time() - start_time, view_name=sanitized_view)

    except psycopg2.errors.DuplicateTable as error:
        # View already exists - CREATE OR REPLACE should handle this, but just in case
        logger.warning(f"View '{sanitized_view}' already exists and was replaced.")
    except psycopg2.errors.UndefinedTable as error:
        connection.rollback()
        log_error(error, "create_view", view_name=sanitized_view)
        raise TableError(f"Referenced table in view '{view_name}' does not exist: {error}") from error
    except psycopg2.ProgrammingError as error:
        connection.rollback()
        log_error(error, "create_view", view_name=sanitized_view)
        raise QueryError(f"SQL syntax error in view definition: {error}") from error
    except psycopg2.OperationalError as error:
        connection.rollback()
        log_error(error, "create_view", view_name=sanitized_view)
        raise ConnectionError(f"Connection error while creating view '{view_name}': {error}") from error
    except Exception as error:
        connection.rollback()
        log_error(error, "create_view", view_name=sanitized_view)
        raise SonnixgresError(f"Unexpected error while creating view '{view_name}': {error}") from error
    finally:
        if close_connection:
            try:
                connection.close()
                logger.debug("Database connection closed.")
            except Exception as close_error:
                logger.warning(f"Error closing connection: {close_error}")