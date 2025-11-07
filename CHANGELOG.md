# Changelog

All notable changes to sonnixgres will be documented in this file.

## [0.2.0] - 2025-11-07

### Production Ready Release

This release marks sonnixgres as production-ready with complete feature implementation, security fixes, and comprehensive testing infrastructure.

### Security

- Fixed SQL injection vulnerabilities in identifier sanitization (Issue #11)
  - Case-insensitive SQL keyword detection
  - Strengthened regex patterns to prevent consecutive dots
  - Expanded keyword list from 24 to 70+ SQL keywords
  - Individual validation of dot-separated identifier parts
- Added view query validation to prevent SQL injection
- Renamed .env to .env.example for credential safety

### Added

- Complete exception hierarchy with 27 custom exception classes
  - SonnixgresError base class
  - Specific errors: ConnectionError, QueryError, DataError, TableError, etc.
- Transaction management with context managers
  - transaction_context() for safe transaction handling
  - Automatic rollback on errors
- Comprehensive input validation module (validation.py)
  - 9 validation functions
  - DataFrame validation, table/column name validation
  - Query parameter validation, pagination validation
- Structured logging configuration (logging_config.py)
  - JSON logging with StructuredFormatter
  - Performance logging with log_performance()
  - Error logging with log_error()
  - Query logging with log_query()
- Connection pooling and resource management
  - ThreadedConnectionPool for psycopg2
  - SQLAlchemy engine with QueuePool
  - Connection health checks with check_connection_health()
- Graceful degradation and retry logic
  - retry_on_failure() decorator with exponential backoff
  - Automatic reconnection with _reconnect_on_failure()
  - Configurable max retries and backoff factors
- CI/CD pipeline with GitHub Actions
  - Automated testing on Python 3.9, 3.10, 3.11, 3.12
  - PostgreSQL 15 test database service
  - Code coverage reporting with Codecov
  - 70% minimum coverage threshold
- Test infrastructure
  - pytest configuration with pytest.ini
  - Coverage reporting (HTML, XML, terminal)
  - 25 test functions across 10 test classes

### Fixed

- All missing imports and dependencies (Issue #12)
  - psycopg2, time, pandas properly imported
  - Optional, Union, Dict type hints added
  - All constants defined (DEFAULT_CACHE_TTL, _query_cache)
- Complete core module implementation (Issue #10)
  - PostgresCredentials class fully implemented
  - get_connection() function complete
  - MetadataCache class functional
  - All 13 functions in __init__.py exist in core.py
- Insufficient error handling (Issue #14)
  - Comprehensive exception management
  - Consistent transaction handling
  - Input validation throughout
  - Resource leak prevention
- Test coverage and CI/CD (Issue #15)
  - Added GitHub Actions workflow
  - pytest-cov integration
  - Automated test runs on push/PR

### Performance

- Implemented SQLAlchemy connection pooling for optimal resource management
- Added thread-safe query result caching with configurable TTL
- Memory-efficient streaming queries with configurable chunk sizes
- Batch operations for optimized data insertion
- Automatic SQL type mapping from DataFrame dtypes

### Documentation

- Complete README.md with API reference
- Installation and configuration instructions
- Usage examples and quick start guide
- Error handling documentation
- Testing and development guidelines

## [0.1.5] - 2025-11-06

### Initial Release

- Basic PostgreSQL operations
- Rich console output with formatting
- Environment-based configuration
- DataFrame support for data operations
