"""Module for querying against Snowflake database."""

from typing import Any, Dict, List, Optional, Tuple, Union

from prefect import task
from snowflake.connector.cursor import SnowflakeCursor

from prefect_snowflake.credentials import SnowflakeCredentials


@task
def snowflake_query(
    query: str,
    snowflake_credentials: "SnowflakeCredentials",
    params: Union[Tuple[Any], Dict[str, Any]] = None,
    cursor_type: Optional[SnowflakeCursor] = SnowflakeCursor,
    database: Optional[str] = None,
    warehouse: Optional[str] = None,
) -> List[Tuple[Any]]:
    """
    Executes a query against a Snowflake database.

    Args:
        query: The query to execute against the database.
        params: The params to replace the placeholders in the query.
        snowflake_credentials: The credentials to use to authenticate.
        cursor_type: The type of database cursor to use for the query.
        database: The name of the database to use; overrides
            the credentials definition if provided.
        warehouse: The name of the warehouse to use; overrides
            the credentials definition if provided.

    Returns:
        The output of `response.fetchall()`.

    Examples:
        Query Snowflake table with the ID value parameterized.
        ```python
        from prefect import flow
        from prefect_snowflake import SnowflakeCredentials
        from prefect_snowflake.database import snowflake_query


        @flow
        def snowflake_query_flow():
            snowflake_credentials = SnowflakeCredentials(
                account="account",
                user="user",
                password="password",
                database="database",
                warehouse="warehouse",
            )
            result = snowflake_query(
                "SELECT * FROM table WHERE id=%{id_param}s LIMIT 8;",
                snowflake_credentials,
                params={"id_param": 1}
            )
            return result

        snowflake_query_flow()
        ```
    """
    connect_params = {"database": database, "warehouse": warehouse}
    # context manager automatically rolls back failed transactions and closes
    with snowflake_credentials.get_connection(**connect_params) as connection:
        with connection.cursor(cursor_type) as cursor:
            response = cursor.execute(query, params=params)
            result = response.fetchall()
    return result
