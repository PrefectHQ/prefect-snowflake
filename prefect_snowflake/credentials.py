"""Credentials class to authenticate Snowflake."""

from typing import Optional

from prefect.blocks.core import Block
from pydantic import Field, SecretBytes, SecretStr, root_validator


class SnowflakeCredentials(Block):
    """
    Block used to manage authentication with Snowflake.

    Args:
        account (str): The snowflake account name.
        user (str): The user name used to authenticate.
        password (SecretStr): The password used to authenticate.
        private_key (SecretStr): The PEM used to authenticate.
        authenticator (str): The type of authenticator to use for initializing
            connection (oauth, externalbrowser, etc); refer to
            [Snowflake documentation](https://docs.snowflake.com/en/user-guide/python-connector-api.html#connect)
            for details, and note that `externalbrowser` will only
            work in an environment where a browser is available.
        token (SecretStr): The OAuth or JWT Token to provide when
            authenticator is set to OAuth.
        role (str): The name of the default role to use.
        autocommit (bool): Whether to automatically commit.

    Example:
        Load stored Snowflake credentials:
        ```python
        from prefect_snowflake import SnowflakeCredentials
        snowflake_credentials_block = SnowflakeCredentials.load("BLOCK_NAME")
        ```
    """  # noqa E501

    _block_type_name = "Snowflake Credentials"
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/2DxzAeTM9eHLDcRQx1FR34/f858a501cdff918d398b39365ec2150f/snowflake.png?h=250"  # noqa

    account: str = Field(..., description="The snowflake account name")
    user: str = Field(..., description="The user name used to authenticate")
    password: Optional[SecretStr] = Field(
        default=None, description="The password used to authenticate"
    )
    private_key: Optional[SecretBytes] = Field(
        default=None, description="The PEM used to authenticate"
    )
    authenticator: Optional[str] = Field(
        default=None,
        description=(
            "The type of authenticator to use for initializing "
            "connection (oauth, externalbrowser, etc)"
        ),
    )
    token: Optional[SecretStr] = Field(
        default=None,
        description=(
            "The OAuth or JWT Token to provide when " "authenticator is set to oauth"
        ),
    )
    role: Optional[str] = Field(
        default=None, description="The name of the default role to use"
    )
    autocommit: Optional[bool] = Field(
        default=None, description="Whether to automatically commit"
    )

    @root_validator(pre=True)
    def _validate_auth_kwargs(cls, values):
        """
        Ensure an authorization value has been provided by the user.
        """
        auth_params = ("password", "private_key", "authenticator", "token")
        if not any(values.get(param) for param in auth_params):
            auth_str = ", ".join(auth_params)
            raise ValueError(
                f"One of the authentication keys must be provided: {auth_str}\n"
            )
        return values
