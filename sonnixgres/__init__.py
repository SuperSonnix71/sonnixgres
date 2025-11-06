"""Sonnixgres - A Python module for PostgreSQL database interactions with rich console output."""

from .core import (
    create_connection,
    query_database,
    save_results_to_csv,
    display_results_as_table,
    create_table,
    populate_table,
    update_records,
    create_view,
    MetadataCache,
    ConnectionError,
)

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
    "MetadataCache",
    "ConnectionError",
]