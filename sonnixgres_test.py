# test_sonnixgres.py
import pytest
import pandas as pd
from sonnixgres import create_connection, create_table, populate_table, query_database, update_records, create_view, display_results_as_table, save_results_to_csv

# Constants for testing
TABLE_NAME = "test_table"
VIEW_NAME = "test_view"
CSV_FILENAME = "test_output.csv"
DISPLAY_LIMIT = 50

@pytest.fixture
def dummy_dataframe():
    """Fixture to create a dummy DataFrame within the display limit."""
    data = {
        'column1': [f'data{i}' for i in range(DISPLAY_LIMIT)],
        'column2': [f'more_data{i}' for i in range(DISPLAY_LIMIT)]
    }
    return pd.DataFrame(data)

# Create a new database connection for each test
@pytest.fixture
def db_connection():
    connection = create_connection()
    yield connection
    connection.close()

def test_create_table(db_connection):
    create_table(db_connection, TABLE_NAME)
    # Add further assertions as needed

def test_populate_table(db_connection, dummy_dataframe):
    populate_table(db_connection, TABLE_NAME, dummy_dataframe)
    # Add further assertions as needed

def test_query_database(db_connection):
    df = query_database(db_connection, f'SELECT * FROM {TABLE_NAME}')
    assert not df.empty, "Query should return data"

def test_update_records(db_connection):
    update_query = f'UPDATE {TABLE_NAME} SET column1 = %s WHERE column2 = %s'
    update_records(db_connection, update_query, ('updated_data', 'more_data1'))
    # Add further assertions as needed

def test_create_view(db_connection):
    # Pass only the SQL statement for the view's content
    view_query = "SELECT * FROM test_table"
    create_view(db_connection, VIEW_NAME, view_query)

def test_display_results_as_table(db_connection, dummy_dataframe):
    display_results_as_table(dummy_dataframe, max_column_width=20)
    # This test will be a visual check of the console output

def test_save_results_to_csv(dummy_dataframe):
    save_results_to_csv(dummy_dataframe, CSV_FILENAME)
    # Add further assertions as needed

# Add additional cleanup steps if needed
