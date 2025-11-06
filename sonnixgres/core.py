"""Core functionality for Sonnixgres PostgreSQL database interactions."""

import os
import logging
import threading
from functools import lru_cache
from typing import Optional, List, Dict, Any, Union
from contextlib import contextmanager

from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, inspect
from sqlalchemy.pool import QueuePool
import pandas as pd
import psycopg2
from psycopg2.extensions import AsIs
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

from .utils import sanitize_sql_identifier, validate_connection_params, validate_query_params, parse_table_list