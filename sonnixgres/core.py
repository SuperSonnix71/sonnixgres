import os
import logging
import threading
import time
from functools import lru_cache
from typing import Optional, List, Dict, Any, Union, Tuple
from contextlib import contextmanager
from dataclasses import dataclass

from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, inspect
from sqlalchemy.pool import QueuePool
import pandas as pd
import psycopg2
from psycopg2.extensions import AsIs, connection
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

from .utils import sanitize_sql_identifier, validate_connection_params, validate_query_params, parse_table_list

load_dotenv()

DEFAULT_DISPLAY_LIMIT = 50
VALID_LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
DEFAULT_DB_PORT = 5432
DEFAULT_CACHE_TTL = 300  # 5 minutes


@dataclass
class QueryResult:
    """Cached query result with metadata."""
    dataframe: pd.DataFrame
    timestamp: float
    ttl: int

    def is_expired(self) -> bool:
        return time.time() - self.timestamp > self.ttl


class QueryCache:
    """Simple in-memory query result cache with TTL."""

    def __init__(self, max_size: int = 100):
        self._cache: Dict[str, QueryResult] = {}
        self._max_size = max_size
        self._lock = threading.Lock()

    def _make_key(self, query: str, params: Optional[Tuple]) -> str:
        """Create cache key from query and parameters."""
        param_str = str(params) if params else ""
        return f"{query}:{param_str}"

    def get(self, query: str, params: Optional[Tuple]) -> Optional[pd.DataFrame]:
        """Get cached result if available and not expired."""
        key = self._make_key(query, params)

        with self._lock:
            if key in self._cache:
                result = self._cache[key]
                if not result.is_expired():
                    logger.debug(f"Cache hit for query: {query[:50]}...")
                    return result.dataframe
                else:
                    # Remove expired entry
                    del self._cache[key]

        return None

    def set(self, query: str, params: Optional[Tuple], dataframe: pd.DataFrame, ttl: int = DEFAULT_CACHE_TTL) -> None:
        """Cache query result with TTL."""
        key = self._make_key(query, params)

        with self._lock:
            # Remove oldest entries if cache is full
            if len(self._cache) >= self._max_size:
                oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k].timestamp)
                del self._cache[oldest_key]

            self._cache[key] = QueryResult(dataframe, time.time(), ttl)
            logger.debug(f"Cached result for query: {query[:50]}...")


# Global query cache instance
_query_cache = QueryCache()