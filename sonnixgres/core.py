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