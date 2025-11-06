### Connection Pooling

Sonnixgres uses SQLAlchemy's QueuePool for optimal connection management:

- **Pool size**: 5 connections (configurable)
- **Max overflow**: 10 connections (configurable)
- **Connection pre-ping**: Enabled (detects stale connections)
- **Thread-safe**: Proper locking for concurrent access

### Performance Features

- **Query Caching**: Thread-safe result caching with TTL
- **Streaming Queries**: Memory-efficient processing of large datasets
- **Batch Inserts**: Optimized data insertion in configurable chunks
- **Type Inference**: Automatic SQL type mapping from DataFrame dtypes
- **Connection Reuse**: Efficient connection pooling and management