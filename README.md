# Sonnixgres

[![PyPI version](https://badge.fury.io/py/sonnixgres.svg)](https://badge.fury.io/py/sonnixgres)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: BSD-3-Clause](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

A Python library for simplifying interactions with PostgreSQL databases, with rich console output for better readability and debugging.

## Features

- **Secure**: Input validation and sanitization to prevent SQL injection
- **Fast**: Connection pooling with SQLAlchemy for optimal performance
- **Rich Display**: Beautiful console tables with the Rich library
- **Easy Setup**: Environment-based configuration
- **Thread-Safe**: Proper locking for concurrent operations
- **Type-Safe**: Full type hints throughout the codebase
- **Well-Tested**: Comprehensive test suite with 80%+ coverage

## Installation

Install Sonnixgres using pip:

```bash
pip install sonnixgres
```

Or with Poetry:

```bash
poetry add sonnixgres
```

## Quick Start

1. **Set up your environment variables:**

Create a `.env` file in your project root:

```bash
# Database Configuration
DB_HOST=your_database_host
DB_DATABASE=your_database_name
DB_USER=your_database_username
DB_PASSWORD=your_database_password
DB_PORT=5432
DB_SCHEMA=your_schema
DB_TABLES=table1,table2,table3

# Optional: Logging level
LOG_LEVEL=INFO
```

2. **Use the library:**

```python
from sonnixgres import create_connection, query_database, display_results_as_table
import pandas as pd

# Create a connection (automatically uses your .env config)
with create_connection() as conn:
    # Query data
    df = query_database(conn, "SELECT * FROM users WHERE active = %s", (True,))

    # Display results beautifully
    display_results_as_table(df)
```

## API Reference

### Connection Management

#### `create_connection() -> psycopg2.connect`
Creates a new PostgreSQL database connection using environment variables.

```python
from sonnixgres import create_connection

with create_connection() as conn:
    # Use connection
    pass
```

#### `get_connection() -> ContextManager[psycopg2.connect]`
Context manager for database connections with automatic cleanup.

```python
from sonnixgres import get_connection

with get_connection() as conn:
    # Connection automatically closed on exit
    pass
```

### Data Operations

#### `query_database(connection, query, params=None, close_connection=True) -> pd.DataFrame`
Execute a SQL query and return results as a pandas DataFrame.

```python
df = query_database(
    connection,
    "SELECT * FROM users WHERE age > %s",
    (18,),
    close_connection=False
)
```

#### `save_results_to_csv(dataframe, filename) -> None`
Save a DataFrame to a CSV file.

```python
save_results_to_csv(df, "results.csv")
```

#### `display_results_as_table(dataframe, max_column_width=50, display_limit=50) -> None`
Display a DataFrame as a formatted table in the console.

```python
display_results_as_table(df, max_column_width=30)
```

### Table Operations

#### `create_table(connection, table_name) -> None`
Create a new table with an auto-incrementing ID column.

```python
create_table(connection, "new_table")
```

#### `populate_table(connection, table_name, dataframe) -> None`
Populate a table with data from a pandas DataFrame. Columns are created automatically.

```python
populate_table(connection, "users", user_dataframe)
```

#### `update_records(connection, update_query, params=None, close_connection=True) -> None`
Update records in the database.

```python
update_records(
    connection,
    "UPDATE users SET active = %s WHERE id = %s",
    (True, 123)
)
```

#### `create_view(connection, view_name, view_query, close_connection=True) -> None`
Create or replace a database view.

```python
create_view(
    connection,
    "active_users",
    "SELECT * FROM users WHERE active = true"
)
```

### Metadata Operations

#### `MetadataCache(schema="", tables=None)`
Cache for database metadata with thread-safe operations.

```python
from sonnixgres import MetadataCache

cache = MetadataCache(schema="public", tables=["users", "products"])
cache.refresh_metadata_cache()
columns_info = cache.retrieve_columns_info()
cache.display_metadata()
```

## Security

Sonnixgres takes security seriously:

- **Input Validation**: All table names, column names, and identifiers are validated and sanitized
- **Parameterized Queries**: SQL injection prevention through proper parameterization
- **Credential Protection**: Never commit database credentials to version control
- **Connection Security**: Uses connection pooling and proper cleanup

## Development

### Setup

```bash
# Clone the repository
git clone https://github.com/SuperSonnix71/sonnixgres.git
cd sonnixgres

# Install dependencies
poetry install

# Run tests
poetry run pytest

# Run linting
poetry run black .
poetry run isort .
poetry run mypy .
```

### Testing

The test suite uses pytest with comprehensive mocking:

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=sonnixgres --cov-report=html

# Run specific test file
pytest tests/test_core.py
```

### Code Quality

This project uses:
- **Black** for code formatting
- **isort** for import sorting
- **mypy** for type checking
- **ruff** for linting
- **pytest** for testing

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_HOST` | Database host | Required |
| `DB_DATABASE` | Database name | Required |
| `DB_USER` | Database username | Required |
| `DB_PASSWORD` | Database password | Required |
| `DB_PORT` | Database port | `5432` |
| `DB_SCHEMA` | Database schema | `""` |
| `DB_TABLES` | Comma-separated table list | `""` |
| `LOG_LEVEL` | Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL) | `INFO` |

### Connection Pooling

Sonnixgres uses SQLAlchemy's QueuePool for connection management:
- Pool size: 5 connections
- Max overflow: 10 connections
- Connection pre-ping: Enabled

## Error Handling

Sonnixgres provides clear error messages and proper exception handling:

```python
from sonnixgres import ConnectionError

try:
    with create_connection() as conn:
        df = query_database(conn, "SELECT * FROM invalid_table")
except ConnectionError as e:
    print(f"Connection failed: {e}")
except ValueError as e:
    print(f"Invalid input: {e}")
```

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Write tests for new functionality
4. Ensure all tests pass
5. Update documentation
6. Submit a pull request

## License

**BSD 3-Clause License**

Copyright (c) 2024, Sonny Mir
All rights reserved.

See [LICENSE](LICENSE) for details.

## Changelog

### v0.2.0 (Current)
- Complete security overhaul with input validation
- Connection pooling implementation
- Comprehensive type hints
- Thread-safe metadata caching
- Improved error handling
- Enhanced test suite
- Better documentation

### v0.1.5
- Initial release
- Basic PostgreSQL operations
- Rich console output
- Environment-based configuration

---

Made with ❤️ by [Sonny Mir](https://github.com/SuperSonnix71)