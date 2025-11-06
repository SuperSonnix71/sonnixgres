"""Tests for Sonnixgres core functionality."""

import os
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, mock_open
from psycopg2.extensions import AsIs

from sonnixgres.core import (
    create_connection,
    query_database,
    save_results_to_csv,
    display_results_as_table,
    create_table,
    populate_table,
    update_records,
    create_view,
    MetadataCache,
    ConnectionError,
    PostgresCredentials,
)
from sonnixgres.utils import sanitize_sql_identifier


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    })


@pytest.fixture
def mock_env():
    """Mock environment variables for testing."""
    env_vars = {
        'DB_HOST': 'localhost',
        'DB_DATABASE': 'testdb',
        'DB_USER': 'testuser',
        'DB_PASSWORD': 'testpass',
        'DB_PORT': '5432',
        'DB_SCHEMA': 'public',
        'DB_TABLES': 'users,products',
        'LOG_LEVEL': 'INFO'
    }
    with patch.dict(os.environ, env_vars):
        yield env_vars


class TestPostgresCredentials:
    """Test PostgresCredentials class."""

    def test_valid_credentials(self, mock_env):
        """Test credentials initialization with valid environment variables."""
        creds = PostgresCredentials()
        assert creds.host == 'localhost'
        assert creds.database == 'testdb'
        assert creds.user == 'testuser'
        assert creds.password == 'testpass'
        assert creds.port == 5432
        assert creds.schema == 'public'
        assert creds.tables == ['users', 'products']

    def test_missing_credentials(self, mock_env):
        """Test that missing credentials raise ValueError."""
        with patch.dict(os.environ, {'DB_HOST': ''}, clear=True):
            with pytest.raises(ValueError, match="Missing required database credentials"):
                PostgresCredentials()


class TestConnectionManagement:
    """Test database connection management."""

    @patch('sonnixgres.core.psycopg2.connect')
    def test_create_connection_success(self, mock_connect, mock_env):
        """Test successful connection creation."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        connection = create_connection()

        mock_connect.assert_called_once_with(
            host='localhost',
            database='testdb',
            user='testuser',
            password='testpass',
            port=5432
        )
        assert connection == mock_connection

    @patch('sonnixgres.core.psycopg2.connect')
    def test_create_connection_with_schema(self, mock_connect, mock_env):
        """Test connection creation with schema setting."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        create_connection()

        mock_cursor.execute.assert_called_once_with("SET search_path TO %s", ('public',))

    @patch('sonnixgres.core.psycopg2.connect')
    def test_create_connection_failure(self, mock_connect, mock_env):
        """Test connection creation failure."""
        mock_connect.side_effect = Exception("Connection failed")

        with pytest.raises(ConnectionError, match="Failed to connect to database"):
            create_connection()


class TestQueryDatabase:
    """Test query_database function."""

    @patch('sonnixgres.core.pd.read_sql')
    def test_query_database_success(self, mock_read_sql, mock_env):
        """Test successful query execution."""
        mock_connection = MagicMock()
        mock_df = MagicMock()
        mock_read_sql.return_value = mock_df

        result = query_database(mock_connection, "SELECT * FROM users", ("param1",))

        mock_read_sql.assert_called_once_with("SELECT * FROM users", mock_connection, params=("param1",))
        assert result == mock_df
        mock_connection.close.assert_called_once()

    def test_query_database_no_connection(self):
        """Test query with no connection."""
        with pytest.raises(ConnectionError, match="No database connection provided"):
            query_database(None, "SELECT * FROM users")

    @patch('sonnixgres.core.validate_query_params')
    def test_query_database_validation(self, mock_validate, mock_env):
        """Test query parameter validation."""
        mock_connection = MagicMock()

        query_database(mock_connection, "SELECT * FROM users WHERE id = %s", (1,))

        mock_validate.assert_called_once_with("SELECT * FROM users WHERE id = %s", (1,))


class TestDataOperations:
    """Test data manipulation operations."""

    def test_save_results_to_csv_success(self, sample_dataframe, tmp_path):
        """Test successful CSV saving."""
        filename = tmp_path / "test_output.csv"
        save_results_to_csv(sample_dataframe, str(filename))

        assert filename.exists()
        # Verify content
        df_read = pd.read_csv(filename)
        pd.testing.assert_frame_equal(df_read, sample_dataframe)

    def test_save_results_to_csv_empty_filename(self, sample_dataframe):
        """Test CSV saving with empty filename."""
        with pytest.raises(ValueError, match="Filename cannot be empty"):
            save_results_to_csv(sample_dataframe, "")

    @patch('sonnixgres.core.Console')
    def test_display_results_as_table(self, mock_console, sample_dataframe):
        """Test table display functionality."""
        mock_console_instance = MagicMock()
        mock_console.return_value = mock_console_instance

        display_results_as_table(sample_dataframe, max_column_width=20)

        mock_console_instance.print.assert_called()

    @patch('sonnixgres.core.Console')
    def test_display_results_as_table_large_data(self, mock_console):
        """Test table display with large dataset."""
        large_df = pd.DataFrame({'col1': list(range(60)), 'col2': list(range(60))})
        mock_console_instance = MagicMock()
        mock_console.return_value = mock_console_instance

        display_results_as_table(large_df)

        # Should print warning about large data
        calls = mock_console_instance.print.call_args_list
        assert any("too large" in str(call) for call in calls)


class TestTableOperations:
    """Test table creation and population operations."""

    def test_create_table_success(self, mock_env):
        """Test successful table creation."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        create_table(mock_connection, 'test_table')

        expected_query = "CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY)"
        mock_cursor.execute.assert_called_once()
        args, kwargs = mock_cursor.execute.call_args
        assert args[0] == expected_query
        assert isinstance(args[1][0], AsIs)
        assert str(args[1][0]) == 'test_table'

    def test_create_table_invalid_name(self):
        """Test table creation with invalid name."""
        mock_connection = MagicMock()

        with pytest.raises(ValueError, match="Invalid identifier"):
            create_table(mock_connection, "invalid-table-name; DROP TABLE users;--")

    def test_populate_table_success(self, mock_env, sample_dataframe):
        """Test successful table population."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        populate_table(mock_connection, 'test_table', sample_dataframe)

        # Should have 3 execute calls: 3 ALTER TABLE + 1 INSERT
        assert mock_cursor.execute.call_count == 4

    def test_populate_table_empty_dataframe(self, mock_env):
        """Test table population with empty DataFrame."""
        mock_connection = MagicMock()
        empty_df = pd.DataFrame()

        with pytest.raises(ValueError, match="DataFrame cannot be empty"):
            populate_table(mock_connection, 'test_table', empty_df)


class TestViewOperations:
    """Test view creation operations."""

    def test_create_view_success(self, mock_env):
        """Test successful view creation."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        create_view(mock_connection, 'test_view', 'SELECT * FROM users')

        expected_query = "CREATE OR REPLACE VIEW %s AS %s"
        mock_cursor.execute.assert_called_once()
        args, kwargs = mock_cursor.execute.call_args
        assert args[0] == expected_query
        assert str(args[1][0]) == 'test_view'
        assert str(args[1][1]) == 'SELECT * FROM users'

    def test_create_view_empty_query(self, mock_env):
        """Test view creation with empty query."""
        mock_connection = MagicMock()

        with pytest.raises(ValueError, match="View query cannot be empty"):
            create_view(mock_connection, 'test_view', '')


class TestMetadataCache:
    """Test MetadataCache class."""

    @patch('sonnixgres.core._get_sqlalchemy_engine')
    def test_metadata_cache_init(self, mock_engine, mock_env):
        """Test MetadataCache initialization."""
        mock_engine_instance = MagicMock()
        mock_engine.return_value = mock_engine_instance

        cache = MetadataCache(schema='public', tables=['users', 'products'])

        assert cache.schema == 'public'
        assert cache.tables == ['users', 'products']
        assert cache.engine == mock_engine_instance

    @patch('sonnixgres.core._get_sqlalchemy_engine')
    def test_refresh_metadata_cache(self, mock_engine, mock_env):
        """Test metadata cache refresh."""
        mock_engine_instance = MagicMock()
        mock_metadata = MagicMock()
        mock_engine_instance.reflect.return_value = mock_metadata
        mock_engine.return_value = mock_engine_instance

        cache = MetadataCache(tables=['users'])
        cache.refresh_metadata_cache()

        assert cache.metadata_cache == mock_metadata


class TestSanitization:
    """Test input sanitization functions."""

    def test_sanitize_valid_identifiers(self):
        """Test sanitization of valid identifiers."""
        assert sanitize_sql_identifier('valid_table') == 'valid_table'
        assert sanitize_sql_identifier('user_data') == 'user_data'
        assert sanitize_sql_identifier('test_schema.table') == 'test_schema.table'

    def test_sanitize_invalid_identifiers(self):
        """Test sanitization rejection of invalid identifiers."""
        invalid_names = [
            'table-name',
            'table name',
            '123table',
            '-table',
            'table;drop',
            'table OR 1=1',
            'select',
            'table.name;drop table users',
        ]

        for name in invalid_names:
            with pytest.raises(ValueError, match="Invalid identifier"):
                sanitize_sql_identifier(name)


class TestErrorHandling:
    """Test error handling throughout the module."""

    def test_connection_error_inheritance(self):
        """Test that ConnectionError is properly defined."""
        assert issubclass(ConnectionError, Exception)

    @patch('sonnixgres.core.psycopg2.connect')
    def test_connection_rollback_on_error(self, mock_connect, mock_env):
        """Test that connections are properly rolled back on errors."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connection.commit.side_effect = Exception("Commit failed")
        mock_connect.return_value = mock_connection

        with pytest.raises(Exception):
            with create_connection() as conn:
                create_table(conn, 'test_table')

        # Should rollback on error
        mock_connection.rollback.assert_called_once()


# Integration test that requires actual database connection
@pytest.mark.integration
class TestIntegration:
    """Integration tests that require a real database connection."""

    def test_full_workflow(self, mock_env):
        """Test a complete workflow from connection to data retrieval."""
        # This would require a real database for full integration testing
        # For now, just test the connection setup
        with patch('sonnixgres.core.psycopg2.connect') as mock_connect:
            mock_connection = MagicMock()
            mock_connect.return_value = mock_connection

            with create_connection() as conn:
                assert conn == mock_connection