"""
Sonnixgres Input Validation - Comprehensive validation for all inputs.
"""

import re
import pandas as pd
from typing import Union, Optional, List, Any, Dict
from .exceptions import ValidationError, DataValidationError, DataTypeError


def validate_connection_params(host: str, database: str, user: str) -> None:
    """
    Validate database connection parameters.

    Args:
        host: Database host
        database: Database name
        user: Database user

    Raises:
        ValidationError: If any parameter is invalid
    """
    if not host or not isinstance(host, str) or not host.strip():
        raise ValidationError("Database host cannot be empty")

    if not database or not isinstance(database, str) or not database.strip():
        raise ValidationError("Database name cannot be empty")

    if not user or not isinstance(user, str) or not user.strip():
        raise ValidationError("Database user cannot be empty")

    # Validate host format (basic check)
    if not re.match(r'^[a-zA-Z0-9.-]+$', host):
        raise ValidationError(f"Invalid database host format: {host}")

    # Validate database and user names (basic SQL identifier check)
    for name, label in [(database, "database name"), (user, "user name")]:
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
            raise ValidationError(f"Invalid {label} format: {name}")


def validate_query_params(query: str, params: Optional[tuple]) -> None:
    """
    Validate SQL query and parameters.

    Args:
        query: SQL query string
        params: Query parameters

    Raises:
        ValidationError: If query or parameters are invalid
    """
    if not query or not isinstance(query, str) or not query.strip():
        raise ValidationError("Query cannot be empty")

    # Check for basic SQL injection patterns (very basic check)
    dangerous_patterns = [
        r';\s*(drop|delete|update|insert|alter|create)\s+',
        r'union\s+select.*--',
        r'/\*.*\*/',
        r';\s*shutdown',
    ]

    query_lower = query.lower()
    for pattern in dangerous_patterns:
        if re.search(pattern, query_lower, re.IGNORECASE):
            raise ValidationError(f"Potentially dangerous SQL pattern detected in query: {pattern}")

    # Count placeholders
    placeholder_count = query.count('%s')

    if params is None:
        if placeholder_count > 0:
            raise ValidationError(f"Query contains {placeholder_count} placeholders but no parameters provided")
        return

    if not isinstance(params, (tuple, list)):
        raise ValidationError("Query parameters must be a tuple or list")

    if len(params) != placeholder_count:
        raise ValidationError(f"Query contains {placeholder_count} placeholders but {len(params)} parameters provided")


def validate_table_name(table_name: str) -> None:
    """
    Validate table name.

    Args:
        table_name: Table name to validate

    Raises:
        ValidationError: If table name is invalid
    """
    if not table_name or not isinstance(table_name, str) or not table_name.strip():
        raise ValidationError("Table name cannot be empty")

    # Basic SQL identifier validation
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', table_name):
        raise ValidationError(f"Invalid table name format: {table_name}")

    # Check for SQL keywords
    sql_keywords = {
        'select', 'insert', 'update', 'delete', 'drop', 'create', 'alter',
        'table', 'column', 'database', 'schema', 'index', 'view', 'trigger',
        'function', 'procedure', 'begin', 'commit', 'rollback', 'union',
        'join', 'where', 'having', 'limit', 'offset'
    }

    if table_name.lower() in sql_keywords:
        raise ValidationError(f"Table name cannot be a SQL keyword: {table_name}")


def validate_column_name(column_name: str) -> None:
    """
    Validate column name.

    Args:
        column_name: Column name to validate

    Raises:
        ValidationError: If column name is invalid
    """
    if not column_name or not isinstance(column_name, str) or not column_name.strip():
        raise ValidationError("Column name cannot be empty")

    # Basic SQL identifier validation
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', column_name):
        raise ValidationError(f"Invalid column name format: {column_name}")

    # Check for SQL keywords
    sql_keywords = {
        'select', 'insert', 'update', 'delete', 'drop', 'create', 'alter',
        'table', 'column', 'from', 'where', 'join', 'group', 'order', 'by',
        'having', 'limit', 'offset', 'union', 'distinct'
    }

    if column_name.lower() in sql_keywords:
        raise ValidationError(f"Column name cannot be a SQL keyword: {column_name}")


def validate_dataframe(dataframe: pd.DataFrame, operation: str = "operation") -> None:
    """
    Validate pandas DataFrame for database operations.

    Args:
        dataframe: DataFrame to validate
        operation: Operation name for error messages

    Raises:
        DataValidationError: If DataFrame is invalid
    """
    if not isinstance(dataframe, pd.DataFrame):
        raise DataValidationError(f"Expected pandas DataFrame for {operation}, got {type(dataframe)}")

    if dataframe.empty:
        raise DataValidationError(f"DataFrame cannot be empty for {operation}")

    if len(dataframe.columns) == 0:
        raise DataValidationError(f"DataFrame must have at least one column for {operation}")

    # Validate column names
    for col in dataframe.columns:
        try:
            validate_column_name(str(col))
        except ValidationError as e:
            raise DataValidationError(f"Invalid column name '{col}' in DataFrame: {e}")

    # Check for null values in critical scenarios
    null_counts = dataframe.isnull().sum()
    if null_counts.sum() > 0:
        # Log warning but don't fail - null handling depends on use case
        from .logging_config import logger
        logger.warning(f"DataFrame contains {null_counts.sum()} null values: {dict(null_counts[null_counts > 0])}")


def validate_pagination_params(limit: Optional[int], offset: Optional[int]) -> None:
    """
    Validate pagination parameters.

    Args:
        limit: Maximum number of rows
        offset: Number of rows to skip

    Raises:
        ValidationError: If parameters are invalid
    """
    if limit is not None:
        if not isinstance(limit, int) or limit <= 0:
            raise ValidationError("Limit must be a positive integer")
        if limit > 1000000:  # Reasonable upper bound
            raise ValidationError("Limit cannot exceed 1,000,000 rows")

    if offset is not None:
        if not isinstance(offset, int) or offset < 0:
            raise ValidationError("Offset must be a non-negative integer")


def validate_cache_params(use_cache: bool, cache_ttl: int) -> None:
    """
    Validate caching parameters.

    Args:
        use_cache: Whether caching is enabled
        cache_ttl: Cache time-to-live in seconds

    Raises:
        ValidationError: If parameters are invalid
    """
    if not isinstance(use_cache, bool):
        raise ValidationError("use_cache must be a boolean")

    if not isinstance(cache_ttl, int) or cache_ttl <= 0:
        raise ValidationError("cache_ttl must be a positive integer")

    if cache_ttl > 86400:  # 24 hours
        raise ValidationError("cache_ttl cannot exceed 24 hours (86400 seconds)")


def validate_connection_object(connection) -> None:
    """
    Validate database connection object.

    Args:
        connection: Connection object to validate

    Raises:
        ValidationError: If connection is invalid
    """
    if connection is None:
        raise ValidationError("Database connection cannot be None")

    # Basic check for psycopg2 connection
    if not hasattr(connection, 'cursor'):
        raise ValidationError("Invalid connection object: missing cursor method")

    if not hasattr(connection, 'commit'):
        raise ValidationError("Invalid connection object: missing commit method")


def validate_view_query(view_query: str) -> None:
    """
    Validate view query.

    Args:
        view_query: SQL query for view definition

    Raises:
        ValidationError: If view query is invalid
    """
    if not view_query or not isinstance(view_query, str) or not view_query.strip():
        raise ValidationError("View query cannot be empty")

    # Basic validation - should contain SELECT
    if not view_query.upper().strip().startswith('SELECT'):
        raise ValidationError("View query must start with SELECT")

    # Check for dangerous operations in view definitions
    dangerous_patterns = [
        r';\s*(drop|delete|update|insert|alter)\s+',
        r';\s*shutdown',
    ]

    for pattern in dangerous_patterns:
        if re.search(pattern, view_query, re.IGNORECASE):
            raise ValidationError(f"Potentially dangerous SQL pattern detected in view query: {pattern}")


def sanitize_sql_identifier(identifier: str) -> str:
    """
    Sanitize SQL identifier to prevent injection attacks.

    Args:
        identifier: Identifier to sanitize

    Returns:
        Sanitized identifier

    Raises:
        ValidationError: If identifier is invalid or contains dangerous patterns
    """
    if not identifier or not isinstance(identifier, str) or not identifier.strip():
        raise ValidationError("Identifier cannot be empty")

    # Strengthened regex: no consecutive dots, must start with letter/underscore
    # Allows: schema.table but not schema..table or .table
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$', identifier):
        raise ValidationError(f"Invalid identifier: {identifier}. "
                            "Identifiers must contain only letters, numbers, and underscores, "
                            "with optional dot-separated schema qualifiers. "
                            "Must start with a letter or underscore.")

    # Comprehensive SQL keywords list (case-insensitive check)
    sql_keywords = {
        'select', 'insert', 'update', 'delete', 'drop', 'create', 'alter',
        'table', 'column', 'database', 'schema', 'index', 'view', 'trigger',
        'function', 'procedure', 'begin', 'commit', 'rollback', 'union',
        'join', 'where', 'having', 'limit', 'offset', 'group', 'order',
        'by', 'from', 'into', 'values', 'set', 'truncate', 'exec', 'execute',
        'grant', 'revoke', 'declare', 'cursor', 'fetch', 'open', 'close',
        'deallocate', 'prepare', 'describe', 'explain', 'show', 'use',
        'rename', 'replace', 'lock', 'unlock', 'merge', 'call', 'return',
        'if', 'else', 'while', 'loop', 'end', 'case', 'when', 'then',
        'exists', 'all', 'any', 'some', 'in', 'between', 'like', 'is',
        'null', 'not', 'and', 'or', 'xor', 'distinct', 'as', 'on'
    }

    # Split by dots and check each part individually (case-insensitive)
    parts = identifier.split('.')
    for part in parts:
        part_lower = part.lower()
        if part_lower in sql_keywords:
            raise ValidationError(f"Identifier part '{part}' is a SQL keyword")

    return identifier


