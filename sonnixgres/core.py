"""Core functionality for Sonnixgres PostgreSQL database interactions."""

import os
import logging
import threading
from functools import lru_cache
from typing import Optional, List, Dict, Any, Union
from contextlib import contextmanager

from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, inspect
from sqlalchemy.pool import QueuePool
import pandas as pd
import psycopg2
from psycopg2.extensions import AsIs
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

from .utils import sanitize_sql_identifier, validate_connection_params, validate_query_params, parse_table_list

# Load environment variables
load_dotenv()

# Constants
DEFAULT_DISPLAY_LIMIT = 50
VALID_LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
DEFAULT_DB_PORT = 5432


class CustomRichHandler(RichHandler):
    """Custom Rich handler for logging with magenta styling."""

    def __init__(self, console: Console = None, **kwargs):
        super().__init__(console=console, **kwargs)
        self.console = console or Console()

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record with magenta styling."""
        message = self.format(record)
        self.console.print(message, style="magenta")


def _setup_logging() -> logging.Logger:
    """Setup logging configuration."""
    log_level_str = os.getenv('LOG_LEVEL', 'INFO').upper()

    if log_level_str not in VALID_LOG_LEVELS:
        raise ValueError(f"Invalid log level: {log_level_str}. Valid options are: {VALID_LOG_LEVELS}")

    log_level = getattr(logging, log_level_str)

    # Remove any existing handlers to avoid duplicates
    logger = logging.getLogger("sonnixgres")
    logger.handlers.clear()

    logger.setLevel(log_level)
    logger.addHandler(CustomRichHandler(rich_tracebacks=True, show_time=False))

    return logger


# Global logger instance
logger = _setup_logging()


class ConnectionError(Exception):
    """Exception raised when database connection fails."""


class PostgresCredentials:
    """PostgreSQL database credentials loaded from environment variables."""

    def __init__(self) -> None:
        """Initialize credentials from environment variables with validation."""
        self.host = os.getenv('DB_HOST')
        self.database = os.getenv('DB_DATABASE')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
        self.port = int(os.getenv('DB_PORT', DEFAULT_DB_PORT))
        self.schema = os.getenv('DB_SCHEMA', '')
        self.tables = parse_table_list(os.getenv('DB_TABLES', ''))

        # Validate required fields
        if not all([self.host, self.database, self.user, self.password]):
            raise ValueError("Missing required database credentials. Please check your .env file.")

        validate_connection_params(self.host, self.database, self.user)


@lru_cache(maxsize=1)
def _get_sqlalchemy_engine() -> Any:
    """
    Get a cached SQLAlchemy engine with connection pooling.

    Returns:
        SQLAlchemy engine instance
    """
    credentials = PostgresCredentials()
    db_url = f"postgresql+psycopg2://{credentials.user}:{credentials.password}@{credentials.host}:{credentials.port}/{credentials.database}"

    return create_engine(
        db_url,
        poolclass=QueuePool,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,  # Verify connections before use
        echo=False
    )


def create_connection() -> psycopg2.connect:
    """
    Create a new PostgreSQL database connection.

    Returns:
        psycopg2 connection object

    Raises:
        ConnectionError: If connection fails
    """
    credentials = PostgresCredentials()

    try:
        connection = psycopg2.connect(
            host=credentials.host,
            database=credentials.database,
            user=credentials.user,
            password=credentials.password,
            port=credentials.port
        )

        if credentials.schema:
            with connection.cursor() as cursor:
                cursor.execute("SET search_path TO %s", (credentials.schema,))
            logger.info(f"Schema set to {credentials.schema}")

        logger.info("Database connection established successfully")
        return connection

    except psycopg2.DatabaseError as e:
        logger.error(f"Database connection failed: {e}")
        raise ConnectionError(f"Failed to connect to database: {e}") from e


@contextmanager
def get_connection():
    """
    Context manager for database connections.

    Yields:
        psycopg2 connection object

    Automatically closes connection on exit.
    """
    connection = None
    try:
        connection = create_connection()
        yield connection
    finally:
        if connection:
            connection.close()
            logger.debug("Database connection closed")


def query_database(
    connection: psycopg2.connect,
    query: str,
    params: Optional[tuple] = None,
    close_connection: bool = True
) -> pd.DataFrame:
    """
    Execute a SQL query and return results as a pandas DataFrame.

    Args:
        connection: Database connection object
        query: SQL query string
        params: Optional query parameters
        close_connection: Whether to close connection after query

    Returns:
        Query results as pandas DataFrame

    Raises:
        ConnectionError: If connection is invalid
        ValueError: If query parameters are invalid
    """
    if not connection:
        raise ConnectionError("No database connection provided")

    validate_query_params(query, params)

    try:
        df = pd.read_sql(query, connection, params=params)
        logger.info(f"Query executed successfully, returned {len(df)} rows")
        return df

    except psycopg2.DatabaseError as e:
        logger.error(f"Query execution failed: {e}")
        raise
    finally:
        if close_connection:
            connection.close()
            logger.debug("Database connection closed")


def save_results_to_csv(dataframe: pd.DataFrame, filename: str) -> None:
    """
    Save a pandas DataFrame to a CSV file.

    Args:
        dataframe: DataFrame to save
        filename: Output filename

    Raises:
        ValueError: If filename is invalid
        IOError: If file cannot be written
    """
    if not filename or not filename.strip():
        raise ValueError("Filename cannot be empty")

    try:
        dataframe.to_csv(filename, index=False)
        logger.info(f"Data saved to {filename} ({len(dataframe)} rows)")

    except Exception as e:
        logger.error(f"Failed to save data to CSV: {e}")
        raise


def display_results_as_table(
    dataframe: pd.DataFrame,
    max_column_width: int = 50,
    display_limit: int = DEFAULT_DISPLAY_LIMIT
) -> None:
    """
    Display a pandas DataFrame as a formatted table in the console.

    Args:
        dataframe: DataFrame to display
        max_column_width: Maximum width for each column
        display_limit: Maximum number of rows to display
    """
    console = Console()

    if len(dataframe) > display_limit:
        console.print(
            f"[yellow]Data is too large! Displaying only the first [red]{display_limit}[/red] rows. "
            f"To view all data, use [green]save_results_to_csv()[/green].[/yellow]"
        )
        display_df = dataframe.head(display_limit)
    else:
        display_df = dataframe

    table = Table(show_header=True, header_style="bold magenta")

    for col in display_df.columns:
        table.add_column(str(col), max_width=max_column_width, overflow="ellipsis")

    for _, row in display_df.iterrows():
        table.add_row(*[str(item) for item in row])

    console.print(table)


def create_table(connection: psycopg2.connect, table_name: str) -> None:
    """
    Create a new table if it doesn't exist.

    Args:
        connection: Database connection
        table_name: Name of the table to create

    Raises:
        ValueError: If table name is invalid
    """
    sanitized_table = sanitize_sql_identifier(table_name)

    try:
        with connection.cursor() as cursor:
            create_query = "CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY)"
            cursor.execute(create_query, (AsIs(sanitized_table),))
            connection.commit()

        logger.info(f"Table '{sanitized_table}' created successfully")

    except psycopg2.DatabaseError as e:
        connection.rollback()
        logger.error(f"Failed to create table '{sanitized_table}': {e}")
        raise


def populate_table(connection: psycopg2.connect, table_name: str, dataframe: pd.DataFrame) -> None:
    """
    Populate a table with data from a pandas DataFrame.

    Dynamically adds columns based on DataFrame structure.

    Args:
        connection: Database connection
        table_name: Name of the table to populate
        dataframe: DataFrame containing the data

    Raises:
        ValueError: If table name is invalid or DataFrame is empty
    """
    if dataframe.empty:
        raise ValueError("DataFrame cannot be empty")

    sanitized_table = sanitize_sql_identifier(table_name)

    try:
        with connection.cursor() as cursor:
            # Add columns dynamically based on DataFrame
            for col in dataframe.columns:
                sanitized_col = sanitize_sql_identifier(col)
                alter_query = "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s TEXT"
                cursor.execute(alter_query, (AsIs(sanitized_table), AsIs(sanitized_col)))

            # Insert data
            columns = [sanitize_sql_identifier(col) for col in dataframe.columns]
            insert_columns = ', '.join(columns)
            placeholders = ', '.join(['%s'] * len(columns))
            insert_query = f"INSERT INTO %s ({insert_columns}) VALUES ({placeholders})"

            cursor.executemany(insert_query, (AsIs(sanitized_table),), dataframe.values.tolist())
            connection.commit()

        logger.info(f"Inserted {len(dataframe)} rows into table '{sanitized_table}'")

    except psycopg2.DatabaseError as e:
        connection.rollback()
        logger.error(f"Failed to populate table '{sanitized_table}': {e}")
        raise


def update_records(
    connection: psycopg2.connect,
    update_query: str,
    params: Optional[tuple] = None,
    close_connection: bool = True
) -> None:
    """
    Update records in the database.

    Args:
        connection: Database connection
        update_query: SQL UPDATE statement
        params: Query parameters
        close_connection: Whether to close connection after update
    """
    if not connection:
        raise ConnectionError("No database connection provided")

    validate_query_params(update_query, params)

    try:
        with connection.cursor() as cursor:
            cursor.execute(update_query, params)
            connection.commit()

        logger.info("Records updated successfully")

    except psycopg2.DatabaseError as e:
        connection.rollback()
        logger.error(f"Update failed: {e}")
        raise
    finally:
        if close_connection:
            connection.close()
            logger.debug("Database connection closed")


def create_view(
    connection: psycopg2.connect,
    view_name: str,
    view_query: str,
    close_connection: bool = True
) -> None:
    """
    Create or replace a database view.

    Args:
        connection: Database connection
        view_name: Name of the view to create
        view_query: SQL query for the view definition
        close_connection: Whether to close connection after creation
    """
    if not connection:
        raise ConnectionError("No database connection provided")

    if not view_query or not view_query.strip():
        raise ValueError("View query cannot be empty")

    sanitized_view = sanitize_sql_identifier(view_name)

    try:
        with connection.cursor() as cursor:
            create_view_query = "CREATE OR REPLACE VIEW %s AS %s"
            cursor.execute(create_view_query, (AsIs(sanitized_view), AsIs(view_query)))
            connection.commit()

        logger.info(f"View '{sanitized_view}' created successfully")

    except psycopg2.DatabaseError as e:
        connection.rollback()
        logger.error(f"Failed to create view '{sanitized_view}': {e}")
        raise
    finally:
        if close_connection:
            connection.close()
            logger.debug("Database connection closed")


class MetadataCache:
    """Cache for database metadata with thread-safe operations."""

    def __init__(self, schema: str = "", tables: Optional[List[str]] = None) -> None:
        """
        Initialize metadata cache.

        Args:
            schema: Database schema name
            tables: List of table names to cache
        """
        self.schema = schema
        self.tables = [sanitize_sql_identifier(table) for table in (tables or [])]
        self.engine = _get_sqlalchemy_engine()
        self.metadata_cache: Optional[MetaData] = None
        self._lock = threading.Lock()

    def refresh_metadata_cache(self) -> None:
        """Refresh the metadata cache by reflecting database schema."""
        with self._lock:
            try:
                metadata = MetaData()
                for table in self.tables:
                    metadata.reflect(
                        bind=self.engine,
                        only=[table],
                        schema=self.schema or None
                    )

                self.metadata_cache = metadata
                logger.info("Metadata cache refreshed successfully")

            except Exception as e:
                logger.error(f"Failed to refresh metadata cache: {e}")
                raise

    def retrieve_columns_info(self) -> Dict[str, List[str]]:
        """
        Retrieve column information for cached tables.

        Returns:
            Dictionary mapping table names to column info lists
        """
        columns_info = {}

        with self._lock:
            try:
                inspector = inspect(self.engine)

                for table_name in self.tables:
                    full_table_name = f"{self.schema}.{table_name}" if self.schema else table_name
                    columns_detail = inspector.get_columns(table_name, schema=self.schema)
                    columns = [f"{col['name']} ({col['type']})" for col in columns_detail]

                    logger.info(f"Columns in {full_table_name}: {', '.join(columns)}")
                    columns_info[full_table_name] = columns

            except Exception as e:
                logger.error(f"Failed to retrieve column info: {e}")
                raise

        return columns_info

    def display_metadata(self) -> None:
        """Display CREATE TABLE statements for cached metadata."""
        if not self.metadata_cache:
            logger.info("Metadata cache is empty. Call refresh_metadata_cache() first.")
            return

        for table_name in self.metadata_cache.tables:
            table = self.metadata_cache.tables[table_name]

            # Generate column definitions
            column_defs = []
            for column in table.columns:
                sql_type = self._map_sqlalchemy_type(column.type)
                col_def = f"{column.name} {sql_type}"
                if column.primary_key:
                    col_def += " PRIMARY KEY"
                column_defs.append(col_def)

            create_statement = f"CREATE TABLE {table_name} (\n    " + ",\n    ".join(column_defs) + "\n);"
            logger.info(create_statement)

    @staticmethod
    def _map_sqlalchemy_type(sqlalchemy_type: Any) -> str:
        """Map SQLAlchemy types to SQL type strings."""
        type_mapping = {
            'INTEGER': 'INT',
            'BIGINT': 'BIGINT',
            'TEXT': 'VARCHAR(255)',
            'BOOLEAN': 'BOOLEAN',
            'DATE': 'DATE',
            'FLOAT': 'FLOAT',
            'DOUBLE': 'DOUBLE PRECISION',
            'SERIAL': 'SERIAL'
        }

        type_str = str(sqlalchemy_type)
        return type_mapping.get(type_str, 'VARCHAR(255)')