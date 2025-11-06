"""Sonnixgres - A Python module for PostgreSQL database interactions with rich console output."""

# Note: This library is in active development. Some advertised features are not yet implemented.
# See https://github.com/SuperSonnix71/sonnixgres/issues for current status.

try:
    from .core import (
        create_connection,
        query_database,
        save_results_to_csv,
        display_results_as_table,
    )
except ImportError:
    # Core functionality not yet implemented
    create_connection = None
    query_database = None
    save_results_to_csv = None
    display_results_as_table = None

try:
    from .core import (
        create_table,
        populate_table,
        update_records,
        create_view,
    )
except ImportError:
    # Table operations not yet implemented
    create_table = None
    populate_table = None
    update_records = None
    create_view = None

# Advanced features not yet implemented
MetadataCache = None
ConnectionError = None

__version__ = "0.2.0"
__author__ = "Sonny Mir"
__email__ = "sonnym@hotmail.se"

__all__ = [
    "create_connection",
    "query_database",
    "save_results_to_csv",
    "display_results_as_table",
    "create_table",
    "populate_table",
    "update_records",
    "create_view",
    # Note: MetadataCache and ConnectionError are not yet implemented
]