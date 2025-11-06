import re
from typing import List, Union


def sanitize_sql_identifier(identifier: str) -> str:
    """
    Sanitize SQL identifier to prevent injection attacks.
    
    Args:
        identifier: SQL identifier (table name, column name, etc.)
        
    Returns:
        Sanitized identifier
        
    Raises:
        ValueError: If identifier is invalid or contains dangerous patterns
    """
    if not identifier:
        raise ValueError("Identifier cannot be empty")

    # Strengthened regex: no consecutive dots, must start with letter/underscore
    # Allows: schema.table but not schema..table or .table
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$', identifier):
        raise ValueError(f"Invalid identifier: {identifier}. "
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
            raise ValueError(f"Identifier part '{part}' is a SQL keyword")

    return identifier


def validate_connection_params(host: str, database: str, user: str) -> None:
    if not host or not host.strip():
        raise ValueError("Database host cannot be empty")

    if not database or not database.strip():
        raise ValueError("Database name cannot be empty")

    if not user or not user.strip():
        raise ValueError("Database user cannot be empty")

    if not re.match(r'^[a-zA-Z0-9.-]+$', host):
        raise ValueError(f"Invalid database host format: {host}")


def parse_table_list(tables_str: str) -> List[str]:
    if not tables_str or not tables_str.strip():
        return []

    tables = [table.strip() for table in tables_str.split(',') if table.strip()]
    return [sanitize_sql_identifier(table) for table in tables]


def validate_query_params(query: str, params: Union[tuple, None]) -> None:
    if not query:
        raise ValueError("Query cannot be empty")

    placeholder_count = query.count('%s')

    if params is None:
        if placeholder_count > 0:
            raise ValueError(f"Query contains {placeholder_count} placeholders but no parameters provided")
        return

    if not isinstance(params, (tuple, list)):
        raise ValueError("Query parameters must be a tuple or list")

    if len(params) != placeholder_count:
        raise ValueError(f"Query contains {placeholder_count} placeholders but {len(params)} parameters provided")
