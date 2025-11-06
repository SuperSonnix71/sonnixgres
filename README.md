#### `get_connection() -> ContextManager[psycopg2.connect]`
Context manager for database connections with automatic cleanup.

```python
from sonnixgres import get_connection

with get_connection() as conn:
    # Connection automatically returned to pool on exit
    pass
```

### Data Operations

#### `query_database(connection, query, params=None, close_connection=True, use_cache=False, cache_ttl=300, limit=None, offset=None) -> pd.DataFrame`
Execute a SQL query and return results as a pandas DataFrame with optional caching and pagination.

```python
# Basic query
df = query_database(conn, "SELECT * FROM users WHERE age > %s", (18,))

# With caching
df = query_database(conn, "SELECT * FROM users", use_cache=True, cache_ttl=600)

# With pagination
df = query_database(conn, "SELECT * FROM users", limit=100, offset=200)
```

#### `query_database_streaming(connection, query, params=None, chunk_size=1000) -> Iterator[pd.DataFrame]`
Execute a SQL query and stream results as DataFrame chunks for memory efficiency.

```python
from sonnixgres import query_database_streaming

for chunk in query_database_streaming(conn, "SELECT * FROM large_table", chunk_size=500):
    # Process each chunk
    process_data(chunk)
```