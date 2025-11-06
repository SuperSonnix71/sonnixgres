import pandas as pd
from sonnixgres import create_connection, create_table, populate_table, display_results_as_table, query_database, get_connection

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
    with get_connection() as connection:
        create_table(connection, 'example_table_1')
        populate_table(connection, 'example_table_1', dummy_data1)

    print("✅ Successfully created and populated table 'example_table_1'")

    with get_connection() as connection:
        df = query_database(connection, "SELECT * FROM example_table_1")

    print(f"📊 Retrieved {len(df)} rows from 'example_table_1'")
    display_results_as_table(df, max_column_width=25)

    dummy_data2 = create_dummy_data(5, 'second')

    try:
        with get_connection() as connection:
            create_table(connection, 'example_table_2')
            populate_table(connection, 'example_table_2', dummy_data2)

        print("✅ Successfully created and populated table 'example_table_2'")

    except Exception as e:
        print(f"❌ Error in test case 3: {e}")

    try:
        with get_connection() as connection:
            populate_table(connection, 'test_table_nonexistent', dummy_data2)
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