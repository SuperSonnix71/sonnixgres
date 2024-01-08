import pandas as pd
from sonnixgres import create_connection, create_table, populate_table, display_results_as_table

# Create a dummy DataFrame
def create_dummy_data(num_rows, suffix):
    return pd.DataFrame({
        'column1': [f'row{i}_data1_{suffix}' for i in range(1, num_rows + 1)],
        'column2': [f'row{i}_data2_{suffix}' for i in range(1, num_rows + 1)],
        'column3': [f'row{i}_data3_{suffix}' for i in range(1, num_rows + 1)]
    })

# Create a connection to the database
connection = create_connection()

try:
    # Test case 1: Create a new table and populate it with initial dummy data
    dummy_data1 = create_dummy_data(3, 'initial')
    create_table(connection, 'test_table_1')
    populate_table(connection, 'test_table_1', dummy_data1)

    # Test case 2: Attempt to create the same table and populate with different data
    dummy_data2 = create_dummy_data(3, 'second')
    create_table(connection, 'test_table_1')  # Should not create a new table
    populate_table(connection, 'test_table_1', dummy_data2)

    # Test case 3: Create a new table without populating, then populate in a separate step
    create_table(connection, 'test_table_2')
    dummy_data3 = create_dummy_data(3, 'separate')
    populate_table(connection, 'test_table_2', dummy_data3)

    # Test case 4: Try to populate a non-existent table (should fail)
    dummy_data4 = create_dummy_data(3, 'nonexistent')
    try:
        populate_table(connection, 'test_table_nonexistent', dummy_data4)
    except Exception as e:
        print(f"Expected error occurred when populating non-existent table: {e}")

    # Display the populated data
    display_results_as_table(dummy_data1, max_column_width=20)

finally:
    # Close the database connection
    if connection is not None:
        connection.close()

    # Optional cleanup code can be added here to drop the test tables
