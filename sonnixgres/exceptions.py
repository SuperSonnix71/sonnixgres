"""
Sonnixgres Exception Hierarchy - Comprehensive error handling for database operations.
"""

from typing import Optional, Dict, Any


class SonnixgresError(Exception):
    """Base exception class for all Sonnixgres errors."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def __str__(self):
        if self.details:
            details_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            return f"{self.message} ({details_str})"
        return self.message


class ConnectionError(SonnixgresError):
    """Raised when database connection operations fail."""
    pass


class AuthenticationError(ConnectionError):
    """Raised when database authentication fails."""
    pass


class ConnectionTimeoutError(ConnectionError):
    """Raised when database connection times out."""
    pass


class ConnectionPoolExhaustedError(ConnectionError):
    """Raised when connection pool is exhausted."""
    pass


class QueryError(SonnixgresError):
    """Base class for query-related errors."""
    pass


class QuerySyntaxError(QueryError):
    """Raised when SQL query has syntax errors."""
    pass


class QueryTimeoutError(QueryError):
    """Raised when query execution times out."""
    pass


class QueryCancelledError(QueryError):
    """Raised when query is cancelled."""
    pass


class DataError(SonnixgresError):
    """Base class for data-related errors."""
    pass


class DataValidationError(DataError):
    """Raised when data validation fails."""
    pass


class DataTypeError(DataError):
    """Raised when data type conversion fails."""
    pass


class TableError(SonnixgresError):
    """Base class for table-related errors."""
    pass


class TableNotFoundError(TableError):
    """Raised when specified table does not exist."""
    pass


class TableAlreadyExistsError(TableError):
    """Raised when attempting to create a table that already exists."""
    pass


class ColumnError(SonnixgresError):
    """Base class for column-related errors."""
    pass


class ColumnNotFoundError(ColumnError):
    """Raised when specified column does not exist."""
    pass


class PermissionError(SonnixgresError):
    """Raised when database permissions are insufficient."""
    pass


class TransactionError(SonnixgresError):
    """Base class for transaction-related errors."""
    pass


class TransactionRollbackError(TransactionError):
    """Raised when transaction rollback fails."""
    pass


class ConfigurationError(SonnixgresError):
    """Raised when configuration is invalid or missing."""
    pass


class ValidationError(SonnixgresError):
    """Raised when input validation fails."""
    pass


class ResourceError(SonnixgresError):
    """Base class for resource-related errors."""
    pass


class ResourceExhaustedError(ResourceError):
    """Raised when system resources are exhausted."""
    pass


class CacheError(SonnixgresError):
    """Base class for cache-related errors."""
    pass


class CacheMissError(CacheError):
    """Raised when cache lookup fails."""
    pass


class CacheExpiredError(CacheError):
    """Raised when cached data has expired."""
    pass


# Legacy alias for backward compatibility
DatabaseConnectionError = ConnectionError