import sonnixgres

def test_metadata_enumeration():
    # Create a PostgresCredentials object
    credentials = sonnixgres.PostgresCredentials()

    # Create a MetadataCache object
    metadata_cache = sonnixgres.MetadataCache(credentials.schema, credentials.tables)

    # Refresh the metadata cache
    metadata_cache.refresh_metadata_cache()

    # Display the metadata cache
    metadata_cache.display_metadata()


def test_query_and_display():
    # Create a psycopg2 connection
    connection = sonnixgres.create_connection()

    # Define SQL query
    query = """
SELECT manufacturer, SUM(active_minutes) AS total_active_minutes, AVG(price) AS avg_price
FROM ai.underutilized_users_ams
GROUP BY manufacturer
ORDER BY avg_price DESC, total_active_minutes DESC
LIMIT 10;
    """

    # Execute the query
    result_df = sonnixgres.query_database(connection, query)

    # Display the results as a table
    sonnixgres.display_results_as_table(result_df)

    # Close the connection
    connection.close()


def main():
    print("Testing Metadata Enumeration...")
    test_metadata_enumeration()

    print("\nTesting Query Execution and Display...")
    test_query_and_display()


if __name__ == "__main__":
    main()
