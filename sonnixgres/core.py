def query_database(
    connection: connection,
    query: str,
    params: Optional[tuple] = None,
    close_connection: bool = True,
    use_cache: bool = False,
    cache_ttl: int = DEFAULT_CACHE_TTL
) -> pd.DataFrame:
    if not connection:
        raise ConnectionError("No database connection provided")

    validate_query_params(query, params)

    # Try cache first if enabled
    if use_cache:
        cached_result = _query_cache.get(query, params)
        if cached_result is not None:
            return cached_result

    start_time = time.time()

    try:
        df = pd.read_sql(query, connection, params=params)
        execution_time = time.time() - start_time

        logger.info(f"Query executed successfully, returned {len(df)} rows in {execution_time:.3f}s")

        # Cache result if enabled
        if use_cache:
            _query_cache.set(query, params, df, cache_ttl)

        return df

    except psycopg2.DatabaseError as e:
        execution_time = time.time() - start_time
        logger.error(f"Query execution failed after {execution_time:.3f}s: {e}")
        raise
    finally:
        if close_connection:
            connection.close()
            logger.debug("Database connection closed")