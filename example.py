import pandas as pd
from sonnixgres import (
    create_connection, get_connection, query_database,
    query_database_streaming, display_results_as_table,
    create_table, populate_table
)

def create_dummy_data(num_rows, suffix):
    return pd.DataFrame({
        'id': range(1, num_rows + 1),
        'column1': [f'row{i}_data1_{suffix}' for i in range(1, num_rows + 1)],
        'column2': [f'row{i}_data2_{suffix}' for i in range(1, num_rows + 1)],
        'column3': [f'row{i}_data3_{suffix}' for i in range(1, num_rows + 1)]
    })

print("🚀 Sonnixgres Performance Example")
print("=" * 50)

# Create sample data
dummy_data1 = create_dummy_data(100, 'performance_test')

try:
    # Demonstrate connection pooling with context manager
    print("📊 Testing connection pooling...")
    with get_connection() as conn:
        create_table(conn, 'performance_test')
        populate_table(conn, 'performance_test', dummy_data1)
        print("✅ Table created and populated using connection pool")

    # Demonstrate query caching
    print("\n⚡ Testing query caching...")
    with get_connection() as conn:
        # First query (will be cached)
        import time
        start = time.time()
        df1 = query_database(conn, "SELECT COUNT(*) as total FROM performance_test", use_cache=True, cache_ttl=60)
        first_query_time = time.time() - start

        # Second query (should use cache)
        start = time.time()
        df2 = query_database(conn, "SELECT COUNT(*) as total FROM performance_test", use_cache=True, cache_ttl=60)
        second_query_time = time.time() - start

        print(f"First query: {first_query_time:.3f}s")
        print(f"Cached query: {second_query_time:.3f}s")
        print(f"Cache speedup: {first_query_time/second_query_time:.1f}x faster")

    # Demonstrate pagination
    print("\n📄 Testing pagination...")
    with get_connection() as conn:
        # Get first 10 rows
        df_page1 = query_database(conn, "SELECT * FROM performance_test ORDER BY id", limit=10, offset=0)
        # Get next 10 rows
        df_page2 = query_database(conn, "SELECT * FROM performance_test ORDER BY id", limit=10, offset=10)

        print(f"Page 1: {len(df_page1)} rows (IDs: {df_page1['id'].min()}-{df_page1['id'].max()})")
        print(f"Page 2: {len(df_page2)} rows (IDs: {df_page2['id'].min()}-{df_page2['id'].max()})")

    # Demonstrate streaming for large datasets
    print("\n🌊 Testing streaming queries...")
    large_data = create_dummy_data(1000, 'streaming_test')

    with get_connection() as conn:
        create_table(conn, 'streaming_test')
        populate_table(conn, 'streaming_test', large_data)

        chunks_processed = 0
        total_rows = 0

        # Process data in chunks to demonstrate streaming
        for chunk in query_database_streaming(conn, "SELECT * FROM streaming_test ORDER BY id", chunk_size=100):
            chunks_processed += 1
            total_rows += len(chunk)
            if chunks_processed <= 3:  # Show first 3 chunks
                print(f"Chunk {chunks_processed}: {len(chunk)} rows (ID range: {chunk['id'].min()}-{chunk['id'].max()})")

        print(f"Total processed: {total_rows} rows in {chunks_processed} chunks")

    print("\n🎉 Performance demonstration completed successfully!")
    print("\n💡 Performance Features Demonstrated:")
    print("   - Connection pooling with automatic resource management")
    print("   - Query result caching with TTL")
    print("   - Pagination support for large datasets")
    print("   - Streaming queries for memory efficiency")
    print("   - Batch operations for optimal data insertion")

except Exception as e:
    print(f"❌ Performance example failed: {e}")
    print("Make sure your .env file is properly configured with database credentials")
    print("Note: This library now includes full performance optimizations")