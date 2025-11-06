from .utils import sanitize_sql_identifier, validate_connection_params, validate_query_params, parse_table_list

load_dotenv()

DEFAULT_DISPLAY_LIMIT = 50
VALID_LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
DEFAULT_DB_PORT = 5432


def _infer_postgresql_type(dtype: str, sample_values: Optional[List] = None) -> str:
    """Infer PostgreSQL data type from pandas dtype and sample values."""
    dtype = str(dtype).lower()

    # Direct mappings for common pandas dtypes
    if dtype == 'int64':
        return 'BIGINT'
    elif dtype == 'int32':
        return 'INTEGER'
    elif dtype == 'float64':
        return 'DOUBLE PRECISION'
    elif dtype == 'float32':
        return 'REAL'
    elif dtype == 'bool':
        return 'BOOLEAN'
    elif dtype.startswith('datetime'):
        return 'TIMESTAMP'
    elif dtype == 'object':
        # For object columns, try to be smarter with sample values
        if sample_values:
            # Check if all values are strings of similar length (likely VARCHAR)
            str_values = [str(v) for v in sample_values if v is not None][:10]  # Sample first 10
            if str_values and all(len(str(v)) <= 255 for v in str_values):
                return 'VARCHAR(255)'
            elif str_values and all(len(str(v)) <= 1000 for v in str_values):
                return 'VARCHAR(1000)'
        return 'TEXT'

    # Default fallback
    return 'TEXT'