"""Pytest configuration and shared fixtures."""

import os
import pytest
from unittest.mock import patch


@pytest.fixture(scope="session", autouse=True)
def mock_env_vars():
    """Mock environment variables for all tests."""
    env_vars = {
        'DB_HOST': 'localhost',
        'DB_DATABASE': 'testdb',
        'DB_USER': 'testuser',
        'DB_PASSWORD': 'testpass',
        'DB_PORT': '5432',
        'DB_SCHEMA': 'public',
        'DB_TABLES': 'users,products',
        'LOG_LEVEL': 'WARNING'  # Reduce log noise during tests
    }

    with patch.dict(os.environ, env_vars, clear=True):
        yield


@pytest.fixture
def mock_connection():
    """Mock database connection for testing."""
    from unittest.mock import MagicMock
    return MagicMock()


@pytest.fixture
def sample_dataframe():
    """Sample DataFrame for testing."""
    import pandas as pd
    return pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    })