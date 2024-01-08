import os
import logging
import warnings
from dotenv import load_dotenv
import pandas as pd
import psycopg2
from psycopg2.extensions import AsIs
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

# Load environment variables
load_dotenv()

class CustomRichHandler(RichHandler):
    def __init__(self, console: Console = None, **kwargs):
        super().__init__(console=console, **kwargs)
        self.console = console or Console()

    def emit(self, record: logging.LogRecord) -> None:
        message = self.format(record)
        self.console.print(message, style="magenta")

log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
valid_log_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

if log_level not in valid_log_levels:
    raise ValueError(f"Invalid log level: {log_level}. Valid options are: {valid_log_levels}")

logging.basicConfig(
    level=log_level,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[CustomRichHandler(rich_tracebacks=True, show_time=False)]
)

logger = logging.getLogger("rich")

warnings.filterwarnings("ignore")

class PostgresCredentials:
    def __init__(self) -> None:
        self.host = os.getenv('DB_HOST')
        self.database = os.getenv('DB_DATABASE')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
        self.port = int(os.getenv('DB_PORT', 5432))
        self.schema = os.getenv('DB_SCHEMA', '')
        self.table = os.getenv('DB_TABLE', '')

class ConnectionError(Exception):
    """Exception raised when connection to database fails."""

def _get_connection(credentials: PostgresCredentials):
    try:
        connection = psycopg2.connect(
            host=credentials.host,
            database=credentials.database,
            user=credentials.user,
            password=credentials.password,
            port=credentials.port
        )

        if credentials.schema:
            with connection.cursor() as cursor:
                cursor.execute("SET search_path TO %s", (credentials.schema,))
            logger.info(f"Schema set to {credentials.schema}")

        return connection
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error getting connection: {error}")
        raise ConnectionError(error) from error

def create_connection() -> psycopg2.connect:
    credentials = PostgresCredentials()
    return _get_connection(credentials)

def query_database(connection: psycopg2.connect, query: str, 
                   params: tuple | None = None, close_connection: bool = True) -> pd.DataFrame:
    if not connection:
        raise ConnectionError("No connection to database.")

    try:
        df = pd.read_sql(query, connection, params=params)
        logger.info("Query executed successfully.")
        return df
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Query execution error: {error}")
        raise
    finally:
        if close_connection:
            connection.close()
            logger.info("Database connection closed.")

def save_results_to_csv(dataframe: pd.DataFrame, filename: str) -> None:
    try:
        dataframe.to_csv(filename, index=False)
        logger.info(f"Data saved to {filename}")
    except Exception as e:
        logger.error(f"Error saving data to CSV: {e}")
        raise

def display_results_as_table(dataframe: pd.DataFrame, max_column_width: int = 50) -> None:
    console = Console()
    display_limit = 50

    if len(dataframe) > display_limit:
        console.print(f"Data is too big! Displaying only the first [red]50[/red] rows. "
                      "To view all data, export it as a CSV using the included function: "
                      "'[green]save_results_to_csv[/green]'.", style="yellow")
        limited_dataframe = dataframe.head(display_limit)
    else:
        limited_dataframe = dataframe

    table = Table(show_header=True, header_style="bold magenta")
    for col in limited_dataframe.columns:
        table.add_column(str(col), max_width=max_column_width)

    for _, row in limited_dataframe.iterrows():
        table.add_row(*[str(item) for item in row])

    console.print(table)
    


def create_table(connection, table_name):
    """Create a new table if it does not exist."""
    try:
        cursor = connection.cursor()
        create_table_query = f"CREATE TABLE IF NOT EXISTS {AsIs(table_name)} ();"
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        logger.info(f"Table '{table_name}' created successfully.")
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error in creating table: {error}")
        raise

def populate_table(connection, table_name, dataframe):
    """Populate the table with data from a DataFrame."""
    try:
        cursor = connection.cursor()

        # Add columns based on DataFrame, one at a time
        for col in dataframe.columns:
            alter_table_query = f"ALTER TABLE {AsIs(table_name)} ADD COLUMN IF NOT EXISTS {col} TEXT;"
            cursor.execute(alter_table_query)

        # Insert data
        insert_columns = ', '.join(dataframe.columns)
        insert_values = ', '.join(['%s'] * len(dataframe.columns))
        insert_query = f"INSERT INTO {AsIs(table_name)} ({insert_columns}) VALUES ({insert_values})"
        cursor.executemany(insert_query, dataframe.values.tolist())
        connection.commit()
        cursor.close()
        logger.info(f"Data inserted into table '{table_name}' successfully.")
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error in populating table: {error}")
        raise





def update_records(connection: psycopg2.connect, update_query: str, 
                   params: tuple | None = None, close_connection: bool = True) -> None:
    if not connection:
        raise ConnectionError("No connection to database.")

    try:
        with connection.cursor() as cursor:
            cursor.execute(update_query, params)
            connection.commit()
            logger.info("Update query executed successfully.")
    except (Exception, psycopg2.DatabaseError) as error:
        connection.rollback()
        logger.error(f"Update query execution error: {error}")
        raise
    finally:
        if close_connection:
            connection.close()
            logger.info("Database connection closed.")

def create_view(connection: psycopg2.connect, view_name: str, view_query: str, 
                close_connection: bool = True) -> None:
    if not connection:
        raise ConnectionError("No connection to database.")

    try:
        with connection.cursor() as cursor:
            create_view_query = f"CREATE OR REPLACE VIEW {AsIs(view_name)} AS {view_query}"
            cursor.execute(create_view_query)
            connection.commit()
            logger.info(f"View '{view_name}' created successfully.")
    except (Exception, psycopg2.DatabaseError) as error:
        connection.rollback()
        logger.error(f"Error creating view '{view_name}': {error}")
        raise
    finally:
        if close_connection:
            connection.close()
            logger.info("Database connection closed.")
