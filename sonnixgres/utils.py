import re
from typing import List, Union


def sanitize_sql_identifier(identifier: str) -> str:
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