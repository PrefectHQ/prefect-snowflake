"""Module for querying against Snowflake database."""

import asyncio
from typing import Any, Dict, List, Optional, Tuple, Union

from prefect import task
from snowflake.connector.cursor import SnowflakeCursor

from prefect_snowflake.credentials import SnowflakeCredentials

BEGIN_TRANSACTION_STATEMENT = "BEGIN TRANSACTION"
END_TRANSACTION_STATEMENT = "COMMIT"


@task
async def snowflake_query(
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
            response = cursor.execute_async(query, params=params)
            query_id = response["queryId"]
            while connection.is_still_running(
                connection.get_query_status_throw_if_error(query_id)
            ):
                await asyncio.sleep(0.05)
            cursor.get_results_from_sfqid(query_id)
            result = cursor.fetchall()
    return result


@task
async def snowflake_multiquery(
    queries: List[str],
    snowflake_credentials: "SnowflakeCredentials",
    params: Union[Tuple[Any], Dict[str, Any]] = None,
    cursor_type: Optional[SnowflakeCursor] = SnowflakeCursor,
    database: Optional[str] = None,
    warehouse: Optional[str] = None,
    as_transaction: bool = False,
    return_transaction_control_results: bool = False,
) -> List[List[Tuple[Any]]]:
    """
    Executes multiple queries against a Snowflake database in a shared session.
    Allows execution in a transaction.

    Args:
        queries: The list of queries to execute against the database.
        params: The params to replace the placeholders in the query.
        snowflake_credentials: The credentials to use to authenticate.
        cursor_type: The type of database cursor to use for the query.
        database: The name of the database to use; overrides
            the credentials definition if provided.
        warehouse: The name of the warehouse to use; overrides
            the credentials definition if provided.
        as_transaction: If True, queries are executed in a transaction.
        return_transaction_control_results: Determines if the results of queries
            controlling the transaction (BEGIN/COMMIT) should be returned.

    Returns:
        List of the outputs of `response.fetchall()` for each query.

    Examples:
        Query Snowflake table with the ID value parameterized.
        ```python
        from prefect import flow
        from prefect_snowflake import SnowflakeCredentials
        from prefect_snowflake.database import snowflake_multiquery


        @flow
        def snowflake_multiquery_flow():
            snowflake_credentials = SnowflakeCredentials(
                account="account",
                user="user",
                password="password",
                database="database",
                warehouse="warehouse",
            )
            result = snowflake_multiquery(
                ["SELECT * FROM table WHERE id=%{id_param}s LIMIT 8;", "SELECT 1,2"],
                snowflake_credentials,
                params={"id_param": 1},
                as_transaction=True
            )
            return result

        snowflake_multiquery_flow()
        ```
    """
    connect_params = {"database": database, "warehouse": warehouse}
    with snowflake_credentials.get_connection(**connect_params) as connection:
        if as_transaction:
            queries.insert(0, BEGIN_TRANSACTION_STATEMENT)
            queries.append(END_TRANSACTION_STATEMENT)

        with connection.cursor(cursor_type) as cursor:
            results = []
            for query in queries:
                response = cursor.execute_async(query, params=params)
                query_id = response["queryId"]
                while connection.is_still_running(
                    connection.get_query_status_throw_if_error(query_id)
                ):
                    await asyncio.sleep(0.05)
                cursor.get_results_from_sfqid(query_id)
                result = cursor.fetchall()
                results.append(result)

    # cut off results from BEGIN/COMMIT queries
    if as_transaction and not return_transaction_control_results:
        return results[1:-1]
    else:
        return results
