"""
Sonnixgres Core Module - High-performance PostgreSQL operations with connection pooling.
"""

import os
import time
import logging
from typing import Optional, Union, Dict, Any, Iterator
from contextlib import contextmanager
import threading

import pandas as pd
import psycopg2
from psycopg2 import pool
from psycopg2.extensions import AsIs
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from sqlalchemy.orm import sessionmaker

from .utils import sanitize_sql_identifier, validate_connection_params, parse_table_list, validate_query_params

# Configure logging
logger = logging.getLogger(__name__)

# Global connection pool
_connection_pool = None
_engine = None
_pool_lock = threading.Lock()

# Query cache
_query_cache: Dict[str, Dict] = {}
_cache_lock = threading.Lock()
DEFAULT_CACHE_TTL = 300  # 5 minutes

# Data type mappings for efficient storage
DTYPE_TO_SQL = {
    'int64': 'BIGINT',
    'int32': 'INTEGER',
    'int16': 'SMALLINT',
    'int8': 'SMALLINT',
    'float64': 'DOUBLE PRECISION',
    'float32': 'REAL',
    'bool': 'BOOLEAN',
    'object': 'TEXT',
    'datetime64[ns]': 'TIMESTAMP',
    'string': 'TEXT'
}


class PostgresCredentials:
    """Credentials class for PostgreSQL connections."""

    def __init__(self):
        self.host = os.getenv('DB_HOST')
        self.database = os.getenv('DB_DATABASE')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
        self.port = int(os.getenv('DB_PORT', '5432'))
        self.schema = os.getenv('DB_SCHEMA', '')
        self.tables = parse_table_list(os.getenv('DB_TABLES', ''))

        # Validate required credentials
        if not all([self.host, self.database, self.user, self.password]):
            raise ValueError("Missing required database credentials: DB_HOST, DB_DATABASE, DB_USER, DB_PASSWORD")

        validate_connection_params(self.host, self.database, self.user)


def _get_connection_pool():
    """Get or create the global connection pool."""
    global _connection_pool
    if _connection_pool is None:
        with _pool_lock:
            if _connection_pool is None:
                creds = PostgresCredentials()
                _connection_pool = pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=20,
                    host=creds.host,
                    database=creds.database,
                    user=creds.user,
                    password=creds.password,
                    port=creds.port
                )
                logger.info("Connection pool initialized")
    return _connection_pool


def _get_sqlalchemy_engine():
    """Get or create the SQLAlchemy engine with connection pooling."""
    global _engine
    if _engine is None:
        with _pool_lock:
            if _engine is None:
                creds = PostgresCredentials()
                db_url = f"postgresql://{creds.user}:{creds.password}@{creds.host}:{creds.port}/{creds.database}"
                _engine = create_engine(
                    db_url,
                    poolclass=QueuePool,
                    pool_size=5,
                    max_overflow=10,
                    pool_pre_ping=True,
                    echo=False
                )
                logger.info("SQLAlchemy engine initialized with connection pooling")
    return _engine


class QueryCache:
    """Thread-safe query result cache."""

    def __init__(self):
        self._cache: Dict[str, Dict] = {}
        self._lock = threading.Lock()

    def _make_key(self, query: str, params: Optional[tuple]) -> str:
        """Create a cache key from query and parameters."""
        params_str = str(params) if params else ""
        return f"{query}:{params_str}"

    def get(self, query: str, params: Optional[tuple]) -> Optional[pd.DataFrame]:
        """Get cached result if available and not expired."""
        key = self._make_key(query, params)
        with self._lock:
            if key in self._cache:
                entry = self._cache[key]
                if time.time() < entry['expires']:
                    logger.debug(f"Cache hit for query: {query[:50]}...")
                    return entry['data']
                else:
                    del self._cache[key]
        return None

    def set(self, query: str, params: Optional[tuple], data: pd.DataFrame, ttl: int):
        """Cache query result with TTL."""
        key = self._make_key(query, params)
        with self._lock:
            self._cache[key] = {
                'data': data.copy(),
                'expires': time.time() + ttl
            }
            logger.debug(f"Cached result for query: {query[:50]}...")


_query_cache = QueryCache()


class ConnectionError(Exception):
    """Custom exception for connection-related errors."""
    pass


def create_connection() -> psycopg2.connect:
    """
    Create a new PostgreSQL database connection using environment variables.

    Returns:
        psycopg2 connection object

    Raises:
        ConnectionError: If connection fails
    """
    try:
        pool = _get_connection_pool()
        conn = pool.getconn()

        # Set schema if specified
        creds = PostgresCredentials()
        if creds.schema:
            with conn.cursor() as cursor:
                cursor.execute("SET search_path TO %s", (creds.schema,))

        logger.debug("Database connection created successfully")
        return conn

    except Exception as e:
        logger.error(f"Failed to create database connection: {e}")
        raise ConnectionError(f"Failed to connect to database: {e}")


@contextmanager
def get_connection():
    """
    Context manager for database connections with automatic cleanup.

    Yields:
        psycopg2 connection object
    """
    conn = None
    try:
        conn = create_connection()
        yield conn
    finally:
        if conn:
            try:
                pool = _get_connection_pool()
                pool.putconn(conn)
                logger.debug("Database connection returned to pool")
            except Exception as e:
                logger.error(f"Error returning connection to pool: {e}")


def query_database(
    connection: psycopg2.connect,
    query: str,
    params: Optional[tuple] = None,
    close_connection: bool = True,
    use_cache: bool = False,
    cache_ttl: int = DEFAULT_CACHE_TTL,
    limit: Optional[int] = None,
    offset: Optional[int] = None
) -> pd.DataFrame:
    """
    Execute a SQL query and return results as a pandas DataFrame.

    Args:
        connection: Database connection
        query: SQL query string
        params: Query parameters
        close_connection: Whether to close connection after query
        use_cache: Whether to use query caching
        cache_ttl: Cache time-to-live in seconds
        limit: Maximum number of rows to return
        offset: Number of rows to skip

    Returns:
        pandas DataFrame with query results
    """
    if not connection:
        raise ConnectionError("No database connection provided")

    validate_query_params(query, params)

    # Modify query for pagination if requested
    if limit is not None or offset is not None:
        if 'LIMIT' in query.upper() or 'OFFSET' in query.upper():
            logger.warning("Query already contains LIMIT/OFFSET, pagination parameters ignored")
        else:
            if limit is not None:
                query += f" LIMIT {limit}"
            if offset is not None:
                query += f" OFFSET {offset}"

    # Try cache first if enabled
    if use_cache:
        cached_result = _query_cache.get(query, params)
        if cached_result is not None:
            return cached_result

    start_time = time.time()

    try:
        df = pd.read_sql(query, connection, params=params)
        execution_time = time.time() - start_time

        logger.info(f"Query executed successfully, returned {len(df)} rows in {execution_time:.3f}s")

        # Cache result if enabled
        if use_cache:
            _query_cache.set(query, params, df, cache_ttl)

        return df

    except psycopg2.DatabaseError as e:
        execution_time = time.time() - start_time
        logger.error(f"Query execution failed after {execution_time:.3f}s: {e}")
        raise
    finally:
        if close_connection:
            try:
                pool = _get_connection_pool()
                pool.putconn(connection)
                logger.debug("Database connection returned to pool")
            except Exception as e:
                logger.error(f"Error returning connection to pool: {e}")


def query_database_streaming(
    connection: psycopg2.connect,
    query: str,
    params: Optional[tuple] = None,
    chunk_size: int = 1000
) -> Iterator[pd.DataFrame]:
    """
    Execute a SQL query and stream results as DataFrame chunks.

    Args:
        connection: Database connection
        query: SQL query string
        params: Query parameters
        chunk_size: Number of rows per chunk

    Yields:
        pandas DataFrame chunks
    """
    if not connection:
        raise ConnectionError("No database connection provided")

    validate_query_params(query, params)

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params or ())

            columns = [desc[0] for desc in cursor.description]
            chunk = []

            for row in cursor:
                chunk.append(row)
                if len(chunk) >= chunk_size:
                    yield pd.DataFrame(chunk, columns=columns)
                    chunk = []

            if chunk:
                yield pd.DataFrame(chunk, columns=columns)

    except psycopg2.DatabaseError as e:
        logger.error(f"Streaming query execution failed: {e}")
        raise


def save_results_to_csv(dataframe: pd.DataFrame, filename: str, **kwargs) -> None:
    """
    Save a DataFrame to a CSV file with optimized settings.

    Args:
        dataframe: DataFrame to save
        filename: Output filename
        **kwargs: Additional pandas to_csv arguments
    """
    if not filename or not filename.strip():
        raise ValueError("Filename cannot be empty")

    if dataframe.empty:
        logger.warning("Attempting to save empty DataFrame")
        return

    default_kwargs = {
        'index': False,
        'encoding': 'utf-8',
        'float_format': '%.6f'
    }
    default_kwargs.update(kwargs)

    dataframe.to_csv(filename, **default_kwargs)
    logger.info(f"DataFrame saved to {filename} ({len(dataframe)} rows)")


def display_results_as_table(
    dataframe: pd.DataFrame,
    max_column_width: int = 50,
    display_limit: int = 50,
    **kwargs
) -> None:
    """
    Display a DataFrame as a formatted table in the console.

    Args:
        dataframe: DataFrame to display
        max_column_width: Maximum width for column display
        display_limit: Maximum rows to display
        **kwargs: Additional rich table arguments
    """
    try:
        from rich.console import Console
        from rich.table import Table

        console = Console()

        if dataframe.empty:
            console.print("[yellow]No data to display[/yellow]")
            return

        # Limit display if needed
        display_df = dataframe.head(display_limit)
        if len(dataframe) > display_limit:
            console.print(f"[dim]Showing first {display_limit} of {len(dataframe)} rows[/dim]")

        table = Table(**kwargs)

        # Add columns
        for col in display_df.columns:
            table.add_column(str(col), max_width=max_column_width)

        # Add rows
        for _, row in display_df.iterrows():
            table.add_row(*[str(val) for val in row])

        console.print(table)

    except ImportError:
        # Fallback to basic pandas display
        logger.warning("Rich library not available, using basic display")
        print(dataframe.head(display_limit))


def _infer_sql_type(dtype: str) -> str:
    """Infer SQL type from pandas dtype."""
    return DTYPE_TO_SQL.get(dtype, 'TEXT')


def create_table(connection: psycopg2.connect, table_name: str) -> None:
    """
    Create a new table with optimized structure.

    Args:
        connection: Database connection
        table_name: Name of table to create
    """
    sanitized_table = sanitize_sql_identifier(table_name)

    try:
        with connection.cursor() as cursor:
            create_table_query = f"CREATE TABLE IF NOT EXISTS {AsIs(sanitized_table)} (id SERIAL PRIMARY KEY);"
            cursor.execute(create_table_query)
            connection.commit()
            logger.info(f"Table '{sanitized_table}' created successfully")
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error creating table: {error}")
        raise


def populate_table(connection: psycopg2.connect, table_name: str, dataframe: pd.DataFrame) -> None:
    """
    Populate a table with data from a DataFrame using optimized data types.

    Args:
        connection: Database connection
        table_name: Target table name
        dataframe: DataFrame to insert
    """
    if dataframe.empty:
        raise ValueError("DataFrame cannot be empty")

    sanitized_table = sanitize_sql_identifier(table_name)
    sanitized_columns = [sanitize_sql_identifier(col) for col in dataframe.columns]

    try:
        with connection.cursor() as cursor:
            # Add columns with inferred types
            for col, dtype in zip(sanitized_columns, dataframe.dtypes):
                sql_type = _infer_sql_type(str(dtype))
                alter_query = f"ALTER TABLE {AsIs(sanitized_table)} ADD COLUMN IF NOT EXISTS {AsIs(col)} {sql_type};"
                cursor.execute(alter_query)

            # Insert data in batches for better performance
            batch_size = 1000
            for i in range(0, len(dataframe), batch_size):
                batch_df = dataframe.iloc[i:i+batch_size]
                insert_columns = ', '.join(sanitized_columns)
                insert_values = ', '.join(['%s'] * len(sanitized_columns))
                insert_query = f"INSERT INTO {AsIs(sanitized_table)} ({insert_columns}) VALUES ({insert_values})"
                cursor.executemany(insert_query, batch_df.values.tolist())

            connection.commit()
            logger.info(f"Data inserted into table '{sanitized_table}' successfully ({len(dataframe)} rows)")

    except (Exception, psycopg2.DatabaseError) as error:
        connection.rollback()
        logger.error(f"Error populating table: {error}")
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
        update_query: UPDATE SQL query
        params: Query parameters
        close_connection: Whether to return connection to pool
    """
    if not connection:
        raise ConnectionError("No connection to database")

    try:
        with connection.cursor() as cursor:
            cursor.execute(update_query, params)
            connection.commit()
            logger.info("Update query executed successfully")
    except (Exception, psycopg2.DatabaseError) as error:
        connection.rollback()
        logger.error(f"Update query execution error: {error}")
        raise
    finally:
        if close_connection:
            try:
                pool = _get_connection_pool()
                pool.putconn(connection)
                logger.debug("Database connection returned to pool")
            except Exception as e:
                logger.error(f"Error returning connection to pool: {e}")


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
        view_name: Name of view to create
        view_query: SQL query for the view
        close_connection: Whether to return connection to pool
    """
    if not connection:
        raise ConnectionError("No database connection provided")

    if not view_query or not view_query.strip():
        raise ValueError("View query cannot be empty")

    sanitized_view = sanitize_sql_identifier(view_name)

    try:
        with connection.cursor() as cursor:
            create_view_query = f"CREATE OR REPLACE VIEW {AsIs(sanitized_view)} AS {view_query}"
            cursor.execute(create_view_query)
            connection.commit()
            logger.info(f"View '{sanitized_view}' created successfully")
    except (Exception, psycopg2.DatabaseError) as error:
        connection.rollback()
        logger.error(f"Error creating view '{sanitized_view}': {error}")
        raise
    finally:
        if close_connection:
            try:
                pool = _get_connection_pool()
                pool.putconn(connection)
                logger.debug("Database connection returned to pool")
            except Exception as e:
                logger.error(f"Error returning connection to pool: {e}")


class MetadataCache:
    """
    Cache for database metadata with thread-safe operations.

    Note: This is a placeholder implementation. Full metadata caching
    will be implemented in a future version.
    """

    def __init__(self, schema: str = "", tables: Optional[list] = None):
        self.schema = schema
        self.tables = tables or []
        self.metadata_cache = None
        self.engine = _get_sqlalchemy_engine()

    def refresh_metadata_cache(self):
        """Refresh the metadata cache."""
        try:
            from sqlalchemy import MetaData
            metadata = MetaData()
            metadata.reflect(bind=self.engine, schema=self.schema)
            self.metadata_cache = metadata
            logger.info("Metadata cache refreshed")
        except Exception as e:
            logger.error(f"Failed to refresh metadata cache: {e}")

    def retrieve_columns_info(self) -> Dict[str, Any]:
        """Retrieve column information from cache."""
        if self.metadata_cache is None:
            self.refresh_metadata_cache()

        if self.metadata_cache:
            return {table: list(self.metadata_cache.tables[table].columns.keys())
                   for table in self.metadata_cache.tables.keys()}
        return {}

    def display_metadata(self):
        """Display cached metadata."""
        columns_info = self.retrieve_columns_info()
        for table, columns in columns_info.items():
            print(f"Table: {table}")
            print(f"Columns: {', '.join(columns)}")
            print("-" * 50)