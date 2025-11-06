"""
Sonnixgres Logging Configuration - Structured logging for database operations.
"""

import os
import logging
import logging.handlers
from typing import Optional
import json
from datetime import datetime


class StructuredFormatter(logging.Formatter):
    """Custom formatter that outputs structured JSON logs."""

    def format(self, record):
        # Add timestamp if not present
        if not hasattr(record, 'timestamp'):
            record.timestamp = datetime.utcnow().isoformat()

        # Create structured log entry
        log_entry = {
            'timestamp': record.timestamp,
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }

        # Add extra fields if present
        if hasattr(record, 'extra_data'):
            log_entry.update(record.extra_data)

        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str)


def setup_logging(
    level: str = "INFO",
    format_type: str = "structured",
    log_file: Optional[str] = None,
    max_bytes: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5
) -> logging.Logger:
    """
    Configure logging for Sonnixgres.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_type: Log format ('structured' for JSON, 'simple' for human-readable)
        log_file: Optional log file path
        max_bytes: Maximum log file size before rotation
        backup_count: Number of backup log files to keep

    Returns:
        Configured logger instance
    """
    # Convert string level to logging level
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    # Create logger
    logger = logging.getLogger('sonnixgres')
    logger.setLevel(numeric_level)

    # Remove existing handlers to avoid duplicates
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Create formatter
    if format_type == 'structured':
        formatter = StructuredFormatter()
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (if specified)
    if log_file:
        # Ensure log directory exists
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def get_logger(name: str = 'sonnixgres') -> logging.Logger:
    """Get a logger instance with the specified name."""
    return logging.getLogger(f'{name}')


# Global logger instance
logger = setup_logging(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format_type=os.getenv('LOG_FORMAT', 'structured'),
    log_file=os.getenv('LOG_FILE')
)


class LogContext:
    """Context manager for adding extra data to log records."""

    def __init__(self, **kwargs):
        self.extra_data = kwargs
        self.old_factory = logging.getLogRecordFactory()

    def __enter__(self):
        def record_factory(*args, **kwargs):
            record = self.old_factory(*args, **kwargs)
            record.extra_data = self.extra_data
            return record

        logging.setLogRecordFactory(record_factory)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.setLogRecordFactory(self.old_factory)


def log_performance(operation: str, duration: float, **kwargs):
    """Log performance metrics."""
    with LogContext(operation=operation, duration=duration, **kwargs):
        logger.info(f"Performance: {operation} completed in {duration:.3f}s")


def log_error(error: Exception, operation: str = None, **kwargs):
    """Log errors with structured information."""
    error_data = {
        'error_type': type(error).__name__,
        'error_message': str(error),
        'operation': operation,
        **kwargs
    }

    with LogContext(**error_data):
        logger.error(f"Error in {operation or 'operation'}: {error}")


def log_query(query: str, params: Optional[tuple] = None, duration: Optional[float] = None):
    """Log database queries with optional parameters and timing."""
    # Sanitize query for logging (remove newlines and extra spaces)
    clean_query = ' '.join(query.split())

    log_data = {
        'query': clean_query,
        'query_length': len(clean_query)
    }

    if params:
        # Don't log actual parameter values for security
        log_data['param_count'] = len(params)

    if duration is not None:
        log_data['duration'] = duration

    with LogContext(**log_data):
        if duration is not None:
            logger.info(f"Query executed in {duration:.3f}s")
        else:
            logger.debug("Query executed")