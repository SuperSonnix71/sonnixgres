import pandas as pd
from sonnixgres import create_connection, create_table, populate_table, display_results_as_table, query_database

def create_dummy_data(num_rows, suffix):
    return pd.DataFrame({
        'column1': [f'row{i}_data1_{suffix}' for i in range(1, num_rows + 1)],
        'column2': [f'row{i}_data2_{suffix}' for i in range(1, num_rows + 1)],
        'column3': [f'row{i}_data3_{suffix}' for i in range(1, num_rows + 1)]
    })

print("🚀 Sonnixgres Example")
print("=" * 50)

dummy_data1 = create_dummy_data(3, 'initial')

try:
    # Create connection and table
    conn1 = create_connection()
    try:
        create_table(conn1, 'example_table_1')
        populate_table(conn1, 'example_table_1', dummy_data1)
        print("✅ Successfully created and populated table 'example_table_1'")
    finally:
        conn1.close()

    # Query data
    conn2 = create_connection()
    try:
        df = query_database(conn2, "SELECT * FROM example_table_1")
        print(f"📊 Retrieved {len(df)} rows from 'example_table_1'")
        display_results_as_table(df, max_column_width=25)
    finally:
        conn2.close()

    dummy_data2 = create_dummy_data(5, 'second')

    try:
        # Create another table
        conn3 = create_connection()
        try:
            create_table(conn3, 'example_table_2')
            populate_table(conn3, 'example_table_2', dummy_data2)
            print("✅ Successfully created and populated table 'example_table_2'")
        finally:
            conn3.close()

    except Exception as e:
        print(f"❌ Error creating second table: {e}")

    try:
        # Test error handling with non-existent table
        conn4 = create_connection()
        try:
            populate_table(conn4, 'test_table_nonexistent', dummy_data2)
        finally:
            conn4.close()
    except Exception as e:
        print(f"Expected error occurred when populating non-existent table: {e}")

    print("\n🎉 Example completed successfully!")
    print("\n💡 Tips:")
    print("   - Check your .env file has correct database credentials")
    print("   - Tables are created with auto-incrementing ID columns")
    print("   - DataFrames are automatically converted to appropriate column types")
    print("   - All database operations include proper error handling and logging")

except Exception as e:
    print(f"❌ Example failed: {e}")
    print("Make sure your .env file is properly configured with database credentials")
    print("Note: This library is in development - some features may not work yet")