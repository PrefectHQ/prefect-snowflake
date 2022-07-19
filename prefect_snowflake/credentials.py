"""Credentials class to authenticate Snowflake."""

from typing import Optional

from prefect.blocks.core import Block
from pydantic import Field, SecretBytes, SecretStr
from snowflake import connector


class SnowflakeCredentials(Block):
    """
    Dataclass used to manage authentication with Snowflake.

    Args:
        account: The snowflake account name.
        user: The user name used to authenticate.
        password: The password used to authenticate.
        private_key: The PEM used to authenticate.
        database: The name of the default database to use.
        warehouse: The name of the default warehouse to use.
        authenticator: The type of authenticator to use for initializing
            connection (oauth, externalbrowser, etc); refer to
            [Snowflake documentation](https://docs.snowflake.com/en/user-guide/python-connector-api.html#connect)
            for details, and note that `externalbrowser` will only
            work in an environment where a browser is available.
        token: The OAuth or JWT Token to provide when
            authenticator is set to OAuth.
        schema: The name of the default schema to use.
        role: The name of the default role to use.
        autocommit: Whether to automatically commit.
    """  # noqa

    account: str
    user: str
    password: Optional[SecretStr] = None
    database: Optional[str] = None
    warehouse: Optional[str] = None
    private_key: Optional[SecretBytes] = None
    authenticator: Optional[str] = None
    token: Optional[SecretStr] = None
    schema_: Optional[str] = Field(alias="schema")
    role: Optional[str] = None
    autocommit: Optional[bool] = None

    def block_initialization(self):
        """
        Filter out unset values.
        """
        connect_params = {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "warehouse": self.warehouse,
            "private_key": self.private_key,
            "authenticator": self.authenticator,
            "token": self.token,
            "schema": self.schema_,
            "role": self.role,
            "autocommit": self.autocommit,
            # required to track task's usage in the Snowflake Partner Network Portal
            "application": "Prefect_Snowflake_Collection",
        }

        # filter out unset values
        self.connect_params = {
            param: value for param, value in connect_params.items() if value is not None
        }

        auth_params = ("password", "private_key", "authenticator", "token")
        if not any(param in self.connect_params for param in auth_params):
            raise ValueError(
                f"One of the authentication methods must be used: {auth_params}"
            )

    def get_connection(
        self, database: Optional[str] = None, warehouse: Optional[str] = None
    ) -> connector.SnowflakeConnection:
        """
        Returns an authenticated connection that can be
        used to query from Snowflake databases.

        Args:
            database: The name of the database to use; overrides
                the class definition if provided.
            warehouse: The name of the warehouse to use; overrides
                the class definition if provided.

        Returns:
            The authenticated SnowflakeConnection.

        Examples:
            ```python
            from prefect import flow
            from prefect_snowflake import SnowflakeCredentials

            @flow
            def snowflake_credentials_flow():
                snowflake_credentials = SnowflakeCredentials(
                    account="account",
                    user="user",
                    password="password",
                    database="database",
                    warehouse="warehouse",
                )
                return snowflake_credentials

            snowflake_credentials_flow()
            ```
        """
        # dont contaminate the original definition
        connect_params = self.connect_params.copy()
        if database is not None:
            connect_params["database"] = database
        if warehouse is not None:
            connect_params["warehouse"] = warehouse

        for param in ("database", "warehouse"):
            if param not in connect_params:
                raise ValueError(
                    f"The {param} must be set in either "
                    f"SnowflakeCredentials or the task"
                )

        connection = connector.connect(**connect_params)
        return connection
