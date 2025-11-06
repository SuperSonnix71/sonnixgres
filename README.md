# Sonnixgres

[![PyPI version](https://badge.fury.io/py/sonnixgres.svg)](https://badge.fury.io/py/sonnixgres)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: BSD-3-Clause](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

A Python library for simplifying interactions with PostgreSQL databases, with rich console output for better readability and debugging.

## ⚠️ Development Status

**This library is currently in active development and not yet production-ready.** Core functionality is incomplete, and the API may change significantly. See [Issues](https://github.com/SuperSonnix71/sonnixgres/issues) for current development status.

## Features

- **Secure**: Input validation and sanitization to prevent SQL injection
- **Rich Display**: Beautiful console tables with the Rich library
- **Easy Setup**: Environment-based configuration
- **Type-Safe**: Type hints throughout the codebase (in progress)

*Note: Advanced features like connection pooling, metadata caching, and comprehensive testing are planned but not yet implemented.*

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

# Optional: Logging level
LOG_LEVEL=INFO
```

2. **Use the library:**

```python
from sonnixgres import create_connection, query_database, display_results_as_table
import pandas as pd

# Create a connection (automatically uses your .env config)
conn = create_connection()

try:
    # Query data
    df = query_database(conn, "SELECT * FROM users WHERE active = %s", (True,))

    # Display results beautifully
    display_results_as_table(df)
finally:
    conn.close()
```

## API Reference

### Connection Management

#### `create_connection() -> psycopg2.connect`
Creates a new PostgreSQL database connection using environment variables.

```python
from sonnixgres import create_connection

conn = create_connection()
# Remember to close the connection when done
conn.close()
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

## Security

Sonnixgres takes security seriously:

- **Input Validation**: All table names, column names, and identifiers are validated and sanitized
- **Parameterized Queries**: SQL injection prevention through proper parameterization
- **Credential Protection**: Never commit database credentials to version control

*Note: Security features are still under development. See [Security Issues](https://github.com/SuperSonnix71/sonnixgres/issues?q=is%3Aissue+is%3Aopen+label%3Asecurity) for current status.*

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

The test suite uses pytest with mocking for isolated testing:

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=sonnixgres --cov-report=html

# Run specific test file
pytest tests/test_core.py
```

*Note: Tests currently use mocking due to incomplete implementations. Integration tests with real databases are planned.*

### Code Quality

This project uses:
- **Black** for code formatting
- **isort** for import sorting
- **mypy** for type checking
- **ruff** for linting
- **pytest** for testing

## Configuration

### Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `DB_HOST` | Database host | - | Yes |
| `DB_DATABASE` | Database name | - | Yes |
| `DB_USER` | Database username | - | Yes |
| `DB_PASSWORD` | Database password | - | Yes |
| `DB_PORT` | Database port | `5432` | No |
| `LOG_LEVEL` | Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL) | `INFO` | No |

## Error Handling

Sonnixgres provides basic error handling:

```python
from sonnixgres import create_connection

try:
    conn = create_connection()
    df = query_database(conn, "SELECT * FROM invalid_table")
except Exception as e:
    print(f"Database operation failed: {e}")
finally:
    if 'conn' in locals():
        conn.close()
```

*Note: Comprehensive error handling and custom exceptions are still under development.*

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Write tests for new functionality
4. Ensure all tests pass
5. Update documentation
6. Submit a pull request

## Troubleshooting

### Common Issues

1. **ImportError**: Make sure all dependencies are installed: `pip install -r requirements.txt`
2. **Connection Failed**: Verify your `.env` file has correct database credentials
3. **Function Not Found**: Some advertised features are not yet implemented (see Issues)

### Getting Help

- Check [Issues](https://github.com/SuperSonnix71/sonnixgres/issues) for known problems
- Create a new issue for bugs or feature requests
- Review the development status before expecting advanced features

## License

**BSD 3-Clause License**

Copyright (c) 2024, Sonny Mir
All rights reserved.

See [LICENSE](LICENSE) for details.

## Changelog

### v0.2.0 (Development)
- Basic PostgreSQL operations
- Input validation and sanitization
- Rich console output
- Environment-based configuration
- Type hints (in progress)

### v0.1.5 (Initial Release)
- Basic PostgreSQL operations
- Rich console output
- Environment-based configuration

---

Made with ❤️ by [Sonny Mir](https://github.com/SuperSonnix71)