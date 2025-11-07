# sonnixgres

A production-ready Python library for PostgreSQL database interactions with connection pooling, query caching, and rich console output.

## Features

- **Connection Pooling**: SQLAlchemy and psycopg2 thread-safe connection pooling
- **Query Caching**: Thread-safe result caching with configurable TTL
- **Streaming Queries**: Memory-efficient processing of large datasets
- **Pagination Support**: Built-in LIMIT/OFFSET for query results
- **Batch Operations**: Optimized data insertion with configurable batch sizes
- **Type Inference**: Automatic SQL type mapping from pandas DataFrames
- **Transaction Management**: Context managers for safe transaction handling
- **Error Handling**: Comprehensive exception hierarchy with retry logic
- **Input Validation**: SQL injection protection and parameter sanitization
- **Logging**: Structured JSON logging with performance monitoring

## Installation

```bash
pip install sonnixgres
```

## Quick Start

```python
import pandas as pd
from sonnixgres import (
    get_connection,
    query_database,
    create_table,
    populate_table
)

# Create a connection using environment variables
with get_connection() as conn:
    # Query with caching
    df = query_database(
        conn,
        "SELECT * FROM users WHERE age > %s",
        params=(25,),
        use_cache=True,
        cache_ttl=300
    )
    print(df)

# Create and populate a table
data = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35]
})

with get_connection() as conn:
    create_table(conn, 'users')
    populate_table(conn, 'users', data)
```

## Configuration

Set the following environment variables:

```bash
DB_HOST=localhost
DB_DATABASE=your_database
DB_USER=your_username
DB_PASSWORD=your_password
DB_PORT=5432
DB_SCHEMA=public
DB_TABLES=table1,table2

# Optional logging configuration
LOG_LEVEL=INFO
LOG_FORMAT=json
LOG_FILE=/path/to/logfile.log
```

Or create a `.env` file:

```ini
DB_HOST=localhost
DB_DATABASE=mydb
DB_USER=admin
DB_PASSWORD=secret
DB_PORT=5432
```

## API Reference

### Connection Management

#### `create_connection()`
Creates a new database connection.

```python
conn = create_connection()
```

#### `get_connection()`
Gets a connection from the pool (context manager).

```python
with get_connection() as conn:
    # Use connection
    pass
```

### Query Operations

#### `query_database(connection, query, params=None, limit=None, offset=None, use_cache=False, cache_ttl=300)`
Execute a query and return results as a DataFrame.

```python
df = query_database(
    conn,
    "SELECT * FROM users WHERE age > %s",
    params=(25,),
    limit=100,
    offset=0,
    use_cache=True,
    cache_ttl=600
)
```

#### `query_database_streaming(connection, query, params=None, chunk_size=1000)`
Stream large query results in chunks.

```python
for chunk in query_database_streaming(conn, "SELECT * FROM large_table", chunk_size=1000):
    process(chunk)
```

### Table Operations

#### `create_table(connection, table_name)`
Create a new table.

```python
create_table(conn, 'users')
```

#### `populate_table(connection, table_name, dataframe)`
Populate a table with DataFrame data.

```python
populate_table(conn, 'users', df)
```

#### `update_records(connection, update_query, params=None)`
Execute an UPDATE query.

```python
update_records(conn, "UPDATE users SET age = %s WHERE id = %s", params=(26, 1))
```

#### `create_view(connection, view_name, view_query)`
Create a database view.

```python
create_view(conn, 'active_users', "SELECT * FROM users WHERE active = true")
```

### Utility Functions

#### `save_results_to_csv(dataframe, filename, **kwargs)`
Save DataFrame to CSV.

```python
save_results_to_csv(df, 'results.csv')
```

#### `display_results_as_table(dataframe, title='Results', max_rows=None)`
Display results in console with rich formatting.

```python
display_results_as_table(df, title='User Data', max_rows=50)
```

### Classes

#### `PostgresCredentials`
Load credentials from environment variables.

```python
from sonnixgres import PostgresCredentials

creds = PostgresCredentials()
print(creds.host, creds.database)
```

#### `MetadataCache`
Cache database metadata for performance.

```python
from sonnixgres import MetadataCache

cache = MetadataCache(engine)
cache.refresh_metadata_cache()
columns = cache.retrieve_columns_info()
```

## Error Handling

sonnixgres provides a comprehensive exception hierarchy:

```python
from sonnixgres import (
    SonnixgresError,
    ConnectionError,
    QueryError,
    DataError,
    TableError,
    ValidationError
)

try:
    with get_connection() as conn:
        df = query_database(conn, "SELECT * FROM users")
except ConnectionError as e:
    print(f"Connection failed: {e}")
except QueryError as e:
    print(f"Query failed: {e}")
except ValidationError as e:
    print(f"Invalid input: {e}")
```

## Testing

Run the test suite:

```bash
# Install test dependencies
pip install -r requirements.txt

# Run tests
pytest

# Run with coverage
pytest --cov=sonnixgres --cov-report=html
```

## Development

```bash
# Clone repository
git clone https://github.com/SuperSonnix71/sonnixgres.git
cd sonnixgres

# Install in development mode
pip install -e .

# Run tests
pytest

# Build distribution
python -m build
```

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## License

BSD-3-Clause License. See LICENSE file for details.

## Author

Sonny Mir (sonnym@hotmail.se)

## Links

- GitHub: https://github.com/SuperSonnix71/sonnixgres
- Issues: https://github.com/SuperSonnix71/sonnixgres/issues
