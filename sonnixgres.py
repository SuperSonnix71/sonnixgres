import os
import logging
import warnings
import re
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, inspect
import pandas as pd
import psycopg2
from psycopg2.extensions import AsIs
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table
import threading
import pickle
# Load environment variables
load_dotenv()

# DEPRECATED: This file is deprecated. Use 'from sonnixgres import ...' instead.
warnings.warn(
    "Direct import from 'sonnixgres.py' is deprecated. "
    "Use 'from sonnixgres import create_connection, query_database, ...' instead. "
    "The legacy sonnixgres.py file will be removed in a future version.",
    DeprecationWarning,
    stacklevel=2
)


def sanitize_sql_identifier(identifier: str) -> str:
    """Sanitize SQL identifiers to prevent SQL injection attacks."""
    if not identifier:
        raise ValueError("Identifier cannot be empty")

    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', identifier):
        raise ValueError(f"Invalid identifier: {identifier}. "
                        "Identifiers must contain only letters, numbers, underscores, and dots, "
                        "and must start with a letter or underscore.")

    sql_keywords = {
        'select', 'insert', 'update', 'delete', 'drop', 'create', 'alter',
        'table', 'column', 'database', 'schema', 'index', 'view', 'trigger',
        'function', 'procedure', 'begin', 'commit', 'rollback', 'union',
        'join', 'where', 'having', 'limit', 'offset'
    }

    identifier_lower = identifier.lower()
    for keyword in sql_keywords:
        if keyword in identifier_lower:
            raise ValueError(f"Identifier cannot contain SQL keyword: {keyword}")

    return identifier