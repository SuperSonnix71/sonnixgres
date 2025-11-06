## Changelog

### v0.2.0 (Current)
- **Performance Overhaul**: Implemented SQLAlchemy connection pooling for optimal resource management
- **Query Caching**: Added thread-safe result caching with TTL support
- **Streaming Queries**: Memory-efficient processing of large datasets with configurable chunk sizes
- **Pagination Support**: LIMIT/OFFSET support for query_database function
- **Batch Operations**: Optimized data insertion with configurable batch sizes
- **Type Inference**: Automatic SQL type mapping from DataFrame dtypes
- **Enhanced Error Handling**: Comprehensive exception management and resource cleanup
- **Thread Safety**: Proper locking for concurrent operations
- **Complete API**: All advertised functions now fully implemented

### v0.1.5 (Initial Release)
- Basic PostgreSQL operations
- Rich console output
- Environment-based configuration