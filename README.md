![](icon.png)
# sonnixgres

A Python module for simplifying interactions with PostgreSQL databases, with rich console output for better readability and debugging.

## Installation

Install `sonnixgres` using pip:

```bash
pip install sonnixgres
```

# Usage

First, ensure that your PostgreSQL credentials are set as environment variables. 

Create a `.env ` file in your CWD with the following database enviornment  variables:

```parameters
DB_HOST=your_database_host
DB_DATABASE=your_database_name
DB_USER=your_database_username
DB_PASSWORD=your_database_password
DB_PORT=5432
DB_SCHEMA=your_database_schema
DB_TABLE=your_database_table
```

`sonnixgres` uses these variables to establish database connections.

```python
from sonnixgres import create_connection, query_database, save_results_to_csv, create_and_populate_table, update_records, create_view
import pandas as pd

# Establish a database connection
connection = create_connection()

# Example usage of each function
```

### Functions

#### `create_connection()`

Establishes a connection to the PostgreSQL database using credentials from environment variables.

#### `query_database(connection, query, params=None, close_connection=True)`

Executes a SQL query on the database and returns the result as a Pandas DataFrame.

- `connection`: The database connection object.
- `query`: SQL query string.
- `params`: Optional parameters for the SQL query.
- `close_connection`: Whether to close the database connection after executing the query.

#### `save_results_to_csv(dataframe, filename)`

Saves a Pandas DataFrame to a CSV file.

- `dataframe`: The DataFrame to be saved.
- `filename`: The name of the file where data will be saved.

#### `create_table(connection, table_name)`

Creates a new table in the database.

- `connection`: The database connection object.
- `table_name`: Name of the table to be created.

#### `populate_table(connection, table_name, dataframe)`

Populates a table with data from a DataFrame.

- `connection`: The database connection object.
- `table_name`: Name of the table to be populated.
- `dataframe`: A pandas DataFrame whose data will be used to populate the table.

#### `update_records(connection, update_query, params=None, close_connection=True)`

Updates records in the database based on a given SQL query.

- `connection`: The database connection object.
- `update_query`: SQL update statement.
- `params`: Parameters for the update query.
- `close_connection`: Whether to close the database connection after executing the query.

#### `create_view(connection, view_name, view_query, close_connection=True)`

Creates a new view in the database.

- `connection`: The database connection object.
- `view_name`: Name of the view to be created.
- `view_query`: SQL query string for creating the view.
- `close_connection`: Whether to close the database connection after creating the view.

#### `display_results_as_table(dataframe, max_column_width=50)`

Displays a pandas DataFrame as a table in the console, with an internal row limit for display.

- `dataframe`: The pandas DataFrame to be displayed.
- `max_column_width`: Optional maximum width for each column in the table, defaulting to 50 characters.rows displayed.

## Behavior

- The function initializes a `rich.console.Console` object to handle the console output.

- It sets a display limit of 50 rows. If the DataFrame has more than 50 rows, it only displays the first 50 rows and prints a message indicating this limit. This message also suggests using the `'save_results_to_csv'` function to view all data.

- The function creates a `rich.table.Table` object with headers (bold and magenta style) based on the DataFrame's columns.

- Each column is added to the table with a specified `max_column_width` to control the display width.

- Rows from the DataFrame (or limited DataFrame, if applicable) are added to the table.

- Finally, the table is printed to the console using the `Console` object.
  
  ![](/home/sonny/Pictures/Screenshots/Screenshot%20from%202024-01-08%2017-17-55.png)
  
  ## License

**BSD License**

# Contributions

Contributions are welcome. Please open an issue or submit a pull request with your improvements.

```python
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
