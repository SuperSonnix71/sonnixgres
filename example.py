#!/usr/bin/env python3
"""
Example usage of the Sonnixgres library.

This script demonstrates how to use Sonnixgres to interact with a PostgreSQL database.
Make sure to set up your .env file with database credentials before running this example.
"""

import pandas as pd
from sonnixgres import (
    create_connection,
    create_table,
    populate_table,
    display_results_as_table,
    query_database,
    get_connection,
)


def create_dummy_data(num_rows: int, suffix: str) -> pd.DataFrame:
    """
    Create a dummy DataFrame for testing.

    Args:
        num_rows: Number of rows to create
        suffix: Suffix to append to data values

    Returns:
        DataFrame with dummy data
    """
    return pd.DataFrame({
        'column1': [f'row{i}_data1_{suffix}' for i in range(1, num_rows + 1)],
        'column2': [f'row{i}_data2_{suffix}' for i in range(1, num_rows + 1)],
        'column3': [f'row{i}_data3_{suffix}' for i in range(1, num_rows + 1)]
    })


def main():
    """Main example function demonstrating Sonnixgres usage."""
    print("🚀 Sonnixgres Example")
    print("=" * 50)

    # Test case 1: Create a new table and populate it with initial dummy data
    print("\n📝 Test Case 1: Creating and populating table with initial data")

    dummy_data1 = create_dummy_data(3, 'initial')

    try:
        with get_connection() as connection:
            create_table(connection, 'example_table_1')
            populate_table(connection, 'example_table_1', dummy_data1)

        print("✅ Successfully created and populated table 'example_table_1'")

    except Exception as e:
        print(f"❌ Error in test case 1: {e}")
        return

    # Test case 2: Query the data back
    print("\n🔍 Test Case 2: Querying data from the table")

    try:
        with get_connection() as connection:
            df = query_database(connection, "SELECT * FROM example_table_1")

        print(f"📊 Retrieved {len(df)} rows from 'example_table_1'")
        display_results_as_table(df, max_column_width=25)

    except Exception as e:
        print(f"❌ Error in test case 2: {e}")
        return

    # Test case 3: Create another table with different data
    print("\n📝 Test Case 3: Creating another table with different data")

    dummy_data2 = create_dummy_data(5, 'second')

    try:
        with get_connection() as connection:
            create_table(connection, 'example_table_2')
            populate_table(connection, 'example_table_2', dummy_data2)

        print("✅ Successfully created and populated table 'example_table_2'")

    except Exception as e:
        print(f"❌ Error in test case 3: {e}")
        return

    # Test case 4: Demonstrate error handling
    print("\n⚠️  Test Case 4: Error handling demonstration")

    try:
        # This should fail due to invalid table name
        with get_connection() as connection:
            create_table(connection, "invalid-table-name; DROP TABLE users;--")

    except ValueError as e:
        print(f"✅ Successfully caught validation error: {e}")

    except Exception as e:
        print(f"❌ Unexpected error: {e}")

    print("\n🎉 Example completed successfully!")
    print("\n💡 Tips:")
    print("   - Check your .env file has correct database credentials")
    print("   - Tables are created with auto-incrementing ID columns")
    print("   - DataFrames are automatically converted to appropriate column types")
    print("   - All database operations include proper error handling and logging")


if __name__ == "__main__":
    main()