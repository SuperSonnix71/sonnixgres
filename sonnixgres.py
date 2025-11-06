import os
import logging
import warnings
import re
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, inspect
import pandas as pd
import psycopg2
from psycopg2.extensions import AsIs
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table
import threading
import pickle
# Load environment variables
load_dotenv()

# DEPRECATED: This file is deprecated. Use 'from sonnixgres import ...' instead.
warnings.warn(
    "Direct import from 'sonnixgres.py' is deprecated. "
    "Use 'from sonnixgres import create_connection, query_database, ...' instead. "
    "The legacy sonnixgres.py file will be removed in a future version.",
    DeprecationWarning,
    stacklevel=2
)