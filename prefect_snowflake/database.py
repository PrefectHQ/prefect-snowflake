"""Module for querying against Snowflake database."""

import asyncio
from typing import Any, Dict, List, Tuple, Union

import snowflake.connector
from prefect import task
from prefect.blocks.core import Block
from pydantic import Field
from snowflake.connector.cursor import SnowflakeCursor

from prefect_snowflake import SnowflakeCredentials

BEGIN_TRANSACTION_STATEMENT = "BEGIN TRANSACTION"
END_TRANSACTION_STATEMENT = "COMMIT"


class SnowflakeConnector(Block):

    """
    Block used to manage connections with Snowflake.

    Args:
        database (str): The name of the default database to use.
        warehouse (str): The name of the default warehouse to use.
        schema (str): The name of the default schema to use;
            this attribute is accessible through `SnowflakeConnector(...).schema_`.
        credentials (SnowflakeCredentials): The credentials to authenticate with Snowflake.

    Example:
        Load stored Snowflake connector:
        ```python
        from prefect_snowflake.database import SnowflakeConnector
        snowflake_connector_block = SnowflakeConnector.load("BLOCK_NAME")
        ```
    """  # noqa

    _block_type_name = "Snowflake Connector"
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/2DxzAeTM9eHLDcRQx1FR34/f858a501cdff918d398b39365ec2150f/snowflake.png?h=250"  # noqa

    database: str = Field(..., descriptions="The name of the default database to use")
    warehouse: str = Field(..., description="The name of the default warehouse to use")
    schema_: str = Field(
        alias="schema", description="The name of the default schema to use"
    )
    credentials: SnowflakeCredentials

    def _get_connect_params(self) -> Dict[str, str]:
        """
        Creates a connect params mapping to pass into get_connection.
        """
        connect_params = {
            "database": self.database,
            "warehouse": self.warehouse,
            "schema": self.schema_,
            # required to track task's usage in the Snowflake Partner Network Portal
            "application": "Prefect_Snowflake_Collection",
            **self.credentials.dict(),
        }

        # filter out unset values
        connect_params = {
            param: value for param, value in connect_params.items() if value is not None
        }

        for param in ("password", "private_key", "token"):
            if param in connect_params:
                connect_params[param] = connect_params[param].get_secret_value()

        # set authenticator to the actual okta_endpoint
        if connect_params.get("authenticator") == "okta_endpoint":
            connect_params["authenticator"] = connect_params.pop("okta_endpoint")

        if "private_key" in connect_params:
            private_der_key = self.credentials.resolve_private_key()
            if private_der_key is not None:
                connect_params["private_key"] = private_der_key
                if "password" in connect_params:
                    connect_params.pop("password")

        return connect_params

    def get_connection(self) -> snowflake.connector.SnowflakeConnection:
        """
        Returns an authenticated connection that can be
        used to query from Snowflake databases.

        Returns:
            The authenticated SnowflakeConnection.

        Examples:
            ```python
            from prefect import flow
            from prefect_snowflake.credentials import SnowflakeCredentials
            from prefect_snowflake.database import SnowflakeConnector


            @flow
            def get_connection_flow():
                snowflake_credentials = SnowflakeCredentials(
                    account="account",
                    user="user",
                    password="password",
                )
                snowflake_connector = SnowflakeConnector(
                    database="database",
                    warehouse="warehouse",
                    schema="schema",
                    credentials=snowflake_credentials
                )
                print(snowflake_connector.get_connection())

            get_connection_flow()
            ```
        """
        connect_params = self._get_connect_params()
        connection = snowflake.connector.connect(**connect_params)
        return connection


@task
async def snowflake_query(
    query: str,
    snowflake_connector: SnowflakeConnector,
    params: Union[Tuple[Any], Dict[str, Any]] = None,
    cursor_type: SnowflakeCursor = SnowflakeCursor,
    poll_frequency_seconds: int = 1,
) -> List[Tuple[Any]]:
    """
    Executes a query against a Snowflake database.

    Args:
        query: The query to execute against the database.
        params: The params to replace the placeholders in the query.
        snowflake_connector: The credentials to use to authenticate.
        cursor_type: The type of database cursor to use for the query.
        poll_frequency_seconds: Number of seconds to wait in between checks for
            run completion.

    Returns:
        The output of `response.fetchall()`.

    Examples:
        Query Snowflake table with the ID value parameterized.
        ```python
        from prefect import flow
        from prefect_snowflake.credentials import SnowflakeCredentials
        from prefect_snowflake.database import SnowflakeConnector, snowflake_query


        @flow
        def snowflake_query_flow():
            snowflake_credentials = SnowflakeCredentials(
                account="account",
                user="user",
                password="password",
            )
            snowflake_connector = SnowflakeConnector(
                database="database",
                warehouse="warehouse",
                schema="schema",
                credentials=snowflake_credentials
            )
            result = snowflake_query(
                "SELECT * FROM table WHERE id=%{id_param}s LIMIT 8;",
                snowflake_connector,
                params={"id_param": 1}
            )
            return result

        snowflake_query_flow()
        ```
    """
    # context manager automatically rolls back failed transactions and closes
    with snowflake_connector.get_connection() as connection:
        with connection.cursor(cursor_type) as cursor:
            response = cursor.execute_async(query, params=params)
            query_id = response["queryId"]
            while connection.is_still_running(
                connection.get_query_status_throw_if_error(query_id)
            ):
                await asyncio.sleep(poll_frequency_seconds)
            cursor.get_results_from_sfqid(query_id)
            result = cursor.fetchall()
    return result


@task
async def snowflake_multiquery(
    queries: List[str],
    snowflake_connector: SnowflakeConnector,
    params: Union[Tuple[Any], Dict[str, Any]] = None,
    cursor_type: SnowflakeCursor = SnowflakeCursor,
    as_transaction: bool = False,
    return_transaction_control_results: bool = False,
    poll_frequency_seconds: int = 1,
) -> List[List[Tuple[Any]]]:
    """
    Executes multiple queries against a Snowflake database in a shared session.
    Allows execution in a transaction.

    Args:
        queries: The list of queries to execute against the database.
        params: The params to replace the placeholders in the query.
        snowflake_connector: The credentials to use to authenticate.
        cursor_type: The type of database cursor to use for the query.
        as_transaction: If True, queries are executed in a transaction.
        return_transaction_control_results: Determines if the results of queries
            controlling the transaction (BEGIN/COMMIT) should be returned.
        poll_frequency_seconds: Number of seconds to wait in between checks for
            run completion.

    Returns:
        List of the outputs of `response.fetchall()` for each query.

    Examples:
        Query Snowflake table with the ID value parameterized.
        ```python
        from prefect import flow
        from prefect_snowflake.credentials import SnowflakeCredentials
        from prefect_snowflake.database import SnowflakeConnector, snowflake_multiquery


        @flow
        def snowflake_multiquery_flow():
            snowflake_credentials = SnowflakeCredentials(
                account="account",
                user="user",
                password="password",
            )
            snowflake_connector = SnowflakeConnector(
                database="database",
                warehouse="warehouse",
                schema="schema",
                credentials=snowflake_credentials
            )
            result = snowflake_multiquery(
                ["SELECT * FROM table WHERE id=%{id_param}s LIMIT 8;", "SELECT 1,2"],
                snowflake_connector,
                params={"id_param": 1},
                as_transaction=True
            )
            return result

        snowflake_multiquery_flow()
        ```
    """
    with snowflake_connector.get_connection() as connection:
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
                    await asyncio.sleep(poll_frequency_seconds)
                cursor.get_results_from_sfqid(query_id)
                result = cursor.fetchall()
                results.append(result)

    # cut off results from BEGIN/COMMIT queries
    if as_transaction and not return_transaction_control_results:
        return results[1:-1]
    else:
        return results


@task
async def snowflake_query_sync(
    query: str,
    snowflake_connector: SnowflakeConnector,
    params: Union[Tuple[Any], Dict[str, Any]] = None,
    cursor_type: SnowflakeCursor = SnowflakeCursor,
) -> List[Tuple[Any]]:
    """
    Executes a query in sync mode against a Snowflake database.

    Args:
        query: The query to execute against the database.
        params: The params to replace the placeholders in the query.
        snowflake_connector: The credentials to use to authenticate.
        cursor_type: The type of database cursor to use for the query.

    Returns:
        The output of `response.fetchall()`.

    Examples:
        Execute a put statement.
        ```python
        from prefect import flow
        from prefect_snowflake.credentials import SnowflakeCredentials
        from prefect_snowflake.database import SnowflakeConnector, snowflake_query


        @flow
        def snowflake_query_sync_flow():
            snowflake_credentials = SnowflakeCredentials(
                account="account",
                user="user",
                password="password",
            )
            snowflake_connector = SnowflakeConnector(
                database="database",
                warehouse="warehouse",
                schema="schema",
                credentials=snowflake_credentials
            )
            result = snowflake_query_sync(
                "put file://afile.csv @mystage;",
                snowflake_connector,
            )
            return result

        snowflake_query_sync_flow()
        ```
    """
    # context manager automatically rolls back failed transactions and closes
    with snowflake_connector.get_connection() as connection:
        with connection.cursor(cursor_type) as cursor:
            cursor.execute(query, params=params)
            result = cursor.fetchall()
    return result
