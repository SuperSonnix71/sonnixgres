"""
Sonnixgres Core Module - High-performance PostgreSQL operations with connection pooling.
"""

import os
import time
import logging
from typing import Optional, Union, Dict, Any, Iterator, Callable
from contextlib import contextmanager
from functools import wraps
import threading

import pandas as pd
import psycopg2
from psycopg2 import pool
from psycopg2.extensions import AsIs
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from sqlalchemy.orm import sessionmaker

from .exceptions import (
    SonnixgresError,
    ConnectionError,
    AuthenticationError,
    ConnectionTimeoutError,
    ConnectionPoolExhaustedError,
    QueryError,
    QuerySyntaxError,
    QueryTimeoutError,
    DataError,
    TableError,
    TableNotFoundError,
    ColumnError,
    PermissionError,
    TransactionError,
    ValidationError,
    ResourceError,
    ResourceExhaustedError
)
from .logging_config import logger, log_error, log_performance, log_query
from .validation import (
    validate_connection_params,
    validate_query_params,
    validate_table_name,
    validate_dataframe,
    validate_pagination_params,
    validate_cache_params,
    validate_view_query
)
from .utils import sanitize_sql_identifier, parse_table_list

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


def retry_on_failure(max_retries: int = 3, backoff_factor: float = 1.5,
                    retryable_exceptions: tuple = (ConnectionError, psycopg2.OperationalError)):
    """
    Decorator to retry operations on failure with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        backoff_factor: Exponential backoff multiplier
        retryable_exceptions: Tuple of exceptions that should trigger retry
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        wait_time = backoff_factor ** attempt
                        logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}, "
                                     f"retrying in {wait_time:.1f}s: {e}")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"All {max_retries + 1} attempts failed for {func.__name__}: {e}")
                        raise
            # This should never be reached, but just in case
            raise last_exception
        return wrapper
    return decorator


def check_connection_health(connection) -> bool:
    """
    Check if a database connection is healthy.

    Args:
        connection: Database connection to check

    Returns:
        True if connection is healthy, False otherwise
    """
    if not connection:
        return False

    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
        return True
    except Exception as e:
        logger.warning(f"Connection health check failed: {e}")
        return False


def _reconnect_on_failure(connection):
    """
    Attempt to reconnect if connection is unhealthy.

    Args:
        connection: Connection to check and potentially replace

    Returns:
        Healthy connection (original or new)
    """
    if not check_connection_health(connection):
        logger.warning("Connection is unhealthy, attempting to reconnect")
        try:
            # Return the old connection to pool
            pool = _get_connection_pool()
            pool.putconn(connection)

            # Get a new connection
            return create_connection()
        except Exception as e:
            logger.error(f"Failed to reconnect: {e}")
            raise ConnectionError("Failed to establish healthy connection") from e
    return connection


@contextmanager
def transaction_context(connection):
    """
    Context manager for database transactions with automatic rollback on error.

    Args:
        connection: Database connection

    Yields:
        Database connection with transaction context

    Raises:
        TransactionError: If transaction fails
    """
    if not connection:
        raise ConnectionError("No database connection provided")

    try:
        yield connection
        connection.commit()
        logger.debug("Transaction committed successfully")
    except Exception as e:
        connection.rollback()
        logger.error(f"Transaction rolled back due to error: {e}")
        if isinstance(e, (psycopg2.OperationalError, psycopg2.ProgrammingError, psycopg2.DataError)):
            # Re-raise database-specific errors
            raise
        else:
            # Wrap other errors in TransactionError
            raise TransactionError(f"Transaction failed: {e}") from e


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
            raise ValidationError("Missing required database credentials: DB_HOST, DB_DATABASE, DB_USER, DB_PASSWORD")

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


@retry_on_failure(max_retries=2, retryable_exceptions=(ConnectionError, psycopg2.OperationalError))
def create_connection():
    """
    Create a new PostgreSQL database connection using environment variables.

    Returns:
        psycopg2 connection object

    Raises:
        ConnectionError: If connection fails
    """
    start_time = time.time()

    try:
        pool = _get_connection_pool()
        conn = pool.getconn()

        # Set schema if specified
        creds = PostgresCredentials()
        if creds.schema:
            with conn.cursor() as cursor:
                cursor.execute("SET search_path TO %s", (creds.schema,))

        logger.debug("Database connection created successfully")
        log_performance("create_connection", time.time() - start_time)
        return conn

    except psycopg2.OperationalError as e:
        log_error(e, "create_connection")
        if "authentication failed" in str(e).lower():
            raise AuthenticationError(f"Database authentication failed: {e}") from e
        elif "timeout" in str(e).lower():
            raise ConnectionTimeoutError(f"Database connection timeout: {e}") from e
        else:
            raise ConnectionError(f"Failed to connect to database: {e}") from e
    except psycopg2.pool.PoolError as e:
        log_error(e, "create_connection")
        raise ConnectionPoolExhaustedError(f"Connection pool exhausted: {e}") from e
    except Exception as e:
        log_error(e, "create_connection")
        raise SonnixgresError(f"Unexpected error creating database connection: {e}") from e


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
    connection,
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
    start_time = time.time()

    # Validate inputs
    if not connection:
        raise ConnectionError("No database connection provided")
    validate_query_params(query, params)
    validate_pagination_params(limit, offset)
    if use_cache:
        validate_cache_params(use_cache, cache_ttl)

    # Check connection health
    connection = _reconnect_on_failure(connection)

    # Modify query for pagination if requested
    original_query = query
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
            log_performance("query_database", time.time() - start_time, cached=True)
            return cached_result

    try:
        df = pd.read_sql(query, connection, params=params)
        execution_time = time.time() - start_time

        logger.info(f"Query executed successfully, returned {len(df)} rows in {execution_time:.3f}s")
        log_query(query, params, execution_time)

        # Cache result if enabled
        if use_cache:
            _query_cache.set(query, params, df, cache_ttl)

        log_performance("query_database", execution_time, rows_returned=len(df))
        return df

    except psycopg2.ProgrammingError as e:
        execution_time = time.time() - start_time
        log_error(e, "query_database", execution_time=execution_time)
        raise QuerySyntaxError(f"SQL syntax error in query: {e}") from e
    except psycopg2.OperationalError as e:
        execution_time = time.time() - start_time
        log_error(e, "query_database", execution_time=execution_time)
        raise ConnectionError(f"Connection error during query execution: {e}") from e
    except psycopg2.DataError as e:
        execution_time = time.time() - start_time
        log_error(e, "query_database", execution_time=execution_time)
        raise DataError(f"Data error in query execution: {e}") from e
    except Exception as e:
        execution_time = time.time() - start_time
        log_error(e, "query_database", execution_time=execution_time)
        raise SonnixgresError(f"Unexpected error during query execution: {e}") from e
    finally:
        if close_connection:
            try:
                pool = _get_connection_pool()
                pool.putconn(connection)
                logger.debug("Database connection returned to pool")
            except Exception as e:
                logger.error(f"Error returning connection to pool: {e}")


def query_database_streaming(
    connection,
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
    start_time = time.time()

    # Validate inputs
    if not connection:
        raise ConnectionError("No database connection provided")
    validate_query_params(query, params)

    # Check connection health
    connection = _reconnect_on_failure(connection)

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

        log_performance("query_database_streaming", time.time() - start_time)

    except psycopg2.ProgrammingError as e:
        log_error(e, "query_database_streaming")
        raise QuerySyntaxError(f"SQL syntax error in streaming query: {e}") from e
    except psycopg2.OperationalError as e:
        log_error(e, "query_database_streaming")
        raise ConnectionError(f"Connection error during streaming query: {e}") from e
    except Exception as e:
        log_error(e, "query_database_streaming")
        raise SonnixgresError(f"Unexpected error during streaming query: {e}") from e


def save_results_to_csv(dataframe: pd.DataFrame, filename: str, **kwargs) -> None:
    """
    Save a DataFrame to a CSV file with optimized settings.

    Args:
        dataframe: DataFrame to save
        filename: Output filename
        **kwargs: Additional pandas to_csv arguments
    """
    start_time = time.time()

    # Validate inputs
    if not filename or not filename.strip():
        raise ValidationError("Filename cannot be empty")

    if dataframe.empty:
        logger.warning("Attempting to save empty DataFrame")
        return

    try:
        default_kwargs = {
            'index': False,
            'encoding': 'utf-8',
            'float_format': '%.6f'
        }
        default_kwargs.update(kwargs)

        dataframe.to_csv(filename, **default_kwargs)
        logger.info(f"DataFrame saved to {filename} ({len(dataframe)} rows)")
        log_performance("save_results_to_csv", time.time() - start_time, rows_saved=len(dataframe))

    except Exception as e:
        log_error(e, "save_results_to_csv", filename=filename)
        raise SonnixgresError(f"Error saving DataFrame to CSV: {e}") from e


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


@retry_on_failure(max_retries=1, retryable_exceptions=(ConnectionError, psycopg2.OperationalError))
def create_table(connection, table_name: str) -> None:
    """
    Create a new table with optimized structure.

    Args:
        connection: Database connection
        table_name: Name of table to create
    """
    start_time = time.time()

    # Validate inputs
    validate_table_name(table_name)

    # Check connection health
    connection = _reconnect_on_failure(connection)

    sanitized_table = sanitize_sql_identifier(table_name)

    try:
        with connection.cursor() as cursor:
            create_table_query = f"CREATE TABLE IF NOT EXISTS {AsIs(sanitized_table)} (id SERIAL PRIMARY KEY);"
            cursor.execute(create_table_query)
            connection.commit()
            logger.info(f"Table '{sanitized_table}' created successfully")

        log_performance("create_table", time.time() - start_time, table_name=sanitized_table)

    except psycopg2.errors.DuplicateTable:
        logger.warning(f"Table '{sanitized_table}' already exists")
    except psycopg2.ProgrammingError as e:
        connection.rollback()
        log_error(e, "create_table", table_name=sanitized_table)
        raise QuerySyntaxError(f"SQL syntax error creating table: {e}") from e
    except psycopg2.OperationalError as e:
        connection.rollback()
        log_error(e, "create_table", table_name=sanitized_table)
        raise ConnectionError(f"Connection error creating table: {e}") from e
    except Exception as e:
        connection.rollback()
        log_error(e, "create_table", table_name=sanitized_table)
        raise SonnixgresError(f"Unexpected error creating table: {e}") from e


@retry_on_failure(max_retries=1, retryable_exceptions=(ConnectionError, psycopg2.OperationalError))
def populate_table(connection, table_name: str, dataframe: pd.DataFrame) -> None:
    """
    Populate a table with data from a DataFrame using optimized data types.

    Args:
        connection: Database connection
        table_name: Target table name
        dataframe: DataFrame to insert
    """
    start_time = time.time()

    # Validate inputs
    validate_table_name(table_name)
    validate_dataframe(dataframe, "populate_table")

    # Check connection health
    connection = _reconnect_on_failure(connection)

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
            total_rows = len(dataframe)

            for i in range(0, total_rows, batch_size):
                batch_df = dataframe.iloc[i:i+batch_size]
                insert_columns = ', '.join(sanitized_columns)
                insert_values = ', '.join(['%s'] * len(sanitized_columns))
                insert_query = f"INSERT INTO {AsIs(sanitized_table)} ({insert_columns}) VALUES ({insert_values})"
                cursor.executemany(insert_query, batch_df.values.tolist())

            connection.commit()
            logger.info(f"Data inserted into table '{sanitized_table}' successfully ({total_rows} rows)")

        log_performance("populate_table", time.time() - start_time,
                       table_name=sanitized_table, rows_inserted=total_rows)

    except psycopg2.errors.UndefinedTable as e:
        connection.rollback()
        log_error(e, "populate_table", table_name=sanitized_table)
        raise TableNotFoundError(f"Table '{table_name}' does not exist") from e
    except psycopg2.ProgrammingError as e:
        connection.rollback()
        log_error(e, "populate_table", table_name=sanitized_table)
        raise QuerySyntaxError(f"SQL syntax error populating table: {e}") from e
    except psycopg2.OperationalError as e:
        connection.rollback()
        log_error(e, "populate_table", table_name=sanitized_table)
        raise ConnectionError(f"Connection error populating table: {e}") from e
    except psycopg2.DataError as e:
        connection.rollback()
        log_error(e, "populate_table", table_name=sanitized_table)
        raise DataError(f"Data error populating table: {e}") from e
    except Exception as e:
        connection.rollback()
        log_error(e, "populate_table", table_name=sanitized_table)
        raise SonnixgresError(f"Unexpected error populating table: {e}") from e


@retry_on_failure(max_retries=1, retryable_exceptions=(ConnectionError, psycopg2.OperationalError))
def update_records(
    connection,
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
    start_time = time.time()

    # Validate inputs
    if not connection:
        raise ConnectionError("No connection to database")
    validate_query_params(update_query, params)

    # Check connection health
    connection = _reconnect_on_failure(connection)

    try:
        with connection.cursor() as cursor:
            cursor.execute(update_query, params)
            connection.commit()
            logger.info("Update query executed successfully")

        log_performance("update_records", time.time() - start_time)

    except psycopg2.errors.UndefinedTable as e:
        connection.rollback()
        log_error(e, "update_records")
        raise TableNotFoundError(f"Table referenced in update query does not exist: {e}") from e
    except psycopg2.ProgrammingError as e:
        connection.rollback()
        log_error(e, "update_records")
        raise QuerySyntaxError(f"SQL syntax error in update query: {e}") from e
    except psycopg2.OperationalError as e:
        connection.rollback()
        log_error(e, "update_records")
        raise ConnectionError(f"Connection error during update: {e}") from e
    except Exception as e:
        connection.rollback()
        log_error(e, "update_records")
        raise TransactionError(f"Transaction failed during update: {e}") from e
    finally:
        if close_connection:
            try:
                pool = _get_connection_pool()
                pool.putconn(connection)
                logger.debug("Database connection returned to pool")
            except Exception as e:
                logger.warning(f"Error returning connection to pool: {e}")


@retry_on_failure(max_retries=1, retryable_exceptions=(ConnectionError, psycopg2.OperationalError))
def create_view(
    connection,
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
    start_time = time.time()

    # Validate inputs
    if not connection:
        raise ConnectionError("No database connection provided")
    validate_table_name(view_name)  # Views use same naming rules as tables
    validate_view_query(view_query)  # Validate view query for SQL injection

    # Check connection health
    connection = _reconnect_on_failure(connection)

    sanitized_view = sanitize_sql_identifier(view_name)

    try:
        with connection.cursor() as cursor:
            create_view_query = f"CREATE OR REPLACE VIEW {AsIs(sanitized_view)} AS {view_query}"
            cursor.execute(create_view_query)
            connection.commit()
            logger.info(f"View '{sanitized_view}' created successfully")

        log_performance("create_view", time.time() - start_time, view_name=sanitized_view)

    except psycopg2.errors.DuplicateTable as e:
        logger.warning(f"View '{sanitized_view}' already exists and was replaced")
    except psycopg2.errors.UndefinedTable as e:
        connection.rollback()
        log_error(e, "create_view", view_name=sanitized_view)
        raise TableNotFoundError(f"Referenced table in view '{view_name}' does not exist: {e}") from e
    except psycopg2.ProgrammingError as e:
        connection.rollback()
        log_error(e, "create_view", view_name=sanitized_view)
        raise QuerySyntaxError(f"SQL syntax error in view definition: {e}") from e
    except psycopg2.OperationalError as e:
        connection.rollback()
        log_error(e, "create_view", view_name=sanitized_view)
        raise ConnectionError(f"Connection error while creating view '{view_name}': {e}") from e
    except Exception as e:
        connection.rollback()
        log_error(e, "create_view", view_name=sanitized_view)
        raise SonnixgresError(f"Unexpected error while creating view '{view_name}': {e}") from e
    finally:
        if close_connection:
            try:
                pool = _get_connection_pool()
                pool.putconn(connection)
                logger.debug("Database connection returned to pool")
            except Exception as e:
                logger.warning(f"Error returning connection to pool: {e}")


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
            raise SonnixgresError(f"Failed to refresh metadata cache: {e}") from e

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
        try:
            columns_info = self.retrieve_columns_info()
            for table, columns in columns_info.items():
                print(f"Table: {table}")
                print(f"Columns: {', '.join(columns)}")
                print("-" * 50)
        except Exception as e:
            logger.error(f"Error displaying metadata: {e}")
            raise SonnixgresError(f"Error displaying metadata: {e}") from e