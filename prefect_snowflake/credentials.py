"""Credentials class to authenticate Snowflake."""

import re
from typing import Optional, Union

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

from prefect.blocks.core import Block
from pydantic import Field, SecretBytes, SecretStr, root_validator, validator

# PEM certificates have the pattern:
#   -----BEGIN PRIVATE KEY-----
#   <- multiple lines of encoded data->
#   -----END PRIVATE KEY-----
#
# The regex capture the header and footer into groups 1 and 3, the body into group 2
# group 1: "header" captures series of hyphens followed by anything that is
#           not a hyphen followed by another string of hyphens
# group 2: "body" capture everything upto the next hyphen
# group 3: "footer" duplicates group 1
_SIMPLE_PEM_CERTIFICATE_REGEX = "^(-+[^-]+-+)([^-]+)(-+[^-]+-+)"


class InvalidPemFormat(Exception):
    """Invalid PEM Format Certificate"""


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
        okta_endpoint (str): The Okta endpoint to use when authenticator is
            set to `okta_endpoint`, e.g. `https://<okta_account_name>.okta.com`.
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
    authenticator: Literal[
        "snowflake",
        "externalbrowser",
        "okta_endpoint",
        "oauth",
        "username_password_mfa",
    ] = Field(  # noqa
        default="snowflake",
        description=("The type of authenticator to use for initializing connection"),
    )
    token: Optional[SecretStr] = Field(
        default=None,
        description=(
            "The OAuth or JWT Token to provide when authenticator is set to `oauth`"
        ),
    )
    endpoint: Optional[str] = Field(
        default=None,
        description=(
            "The Okta endpoint to use when authenticator is set to `okta_endpoint`"
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

    @root_validator(pre=True)
    def _validate_token_kwargs(cls, values):
        """
        Ensure an authorization value has been provided by the user.
        """
        authenticator = values.get("authenticator")
        token = values.get("token")
        if authenticator == "oauth" and not token:
            raise ValueError(
                "If authenticator is set to `oauth`, `token` must be provided"
            )
        return values

    @root_validator(pre=True)
    def _validate_okta_kwargs(cls, values):
        """
        Ensure an authorization value has been provided by the user.
        """
        authenticator = values.get("authenticator")
        okta_endpoint = values.get("okta_endpoint")
        if authenticator == "okta_endpoint" and not okta_endpoint:
            raise ValueError(
                "If authenticator is set to `okta_endpoint`, "
                "`okta_endpoint` must be provided"
            )
        return values

    @validator("private_key")
    def _validate_private_key(cls, private_key):
        """
        Ensure a private_key looks like a PEM format certificate.
        """

        if private_key is None:
            return None

        assert isinstance(private_key, SecretBytes)

        pk = _decode_secret(private_key)

        return None if pk is None else SecretBytes(_compose_pem(pk))

    def resolve_private_key(self) -> Optional[bytes]:
        """
        Converts a PEM encoded private key into a DER binary key

        Returns:
            bytes: DER binary key if private_key has been provided
                    otherwise returns None

        Raises:
            InvalidPemFormat: if `private_key` is not in PEM format
        """

        private_key = _decode_secret(self.private_key)

        if private_key is None:
            return None

        return serialization.load_pem_private_key(
            data=private_key,
            password=_decode_secret(self.password),
            backend=default_backend(),
        ).private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )


def _decode_secret(secret: Union[SecretStr, SecretBytes]) -> Optional[bytes]:
    """
    Decode the provided secret into bytes. If the secret is not a
    string or bytes, or it is whitespace, then return None.

    Args:
        secret: The value to decode

    Returns:
        bytes or None

    """
    if isinstance(secret, (SecretBytes, SecretStr)):
        secret = secret.get_secret_value()

    if not isinstance(secret, (bytes, str)) or len(secret) == 0 or secret.isspace():
        return None

    return secret if isinstance(secret, bytes) else secret.encode()


def _compose_pem(private_key: bytes) -> bytes:
    """Validate structure of PEM certificate.

    The original key passed from Prefect is sometimes malformed.
    This function recomposes the key into a valid key that will
    pass the serialization step when resolving the key to a DER.

    Args:
        private_key (str): A valid PEM format string

    Returns:
        tuple: Tuple containing header, body and footer of the PEM certificate

    Raises:
        InvalidPemFormat: if `private_key` is an invalid format
    """
    pem_parts = re.match(_SIMPLE_PEM_CERTIFICATE_REGEX, private_key.decode())
    if pem_parts is None:
        raise InvalidPemFormat()

    body = "\n".join(re.split(r"\s+", pem_parts[2].strip()))
    # reassemble header+body+footer
    return f"{pem_parts[1]}\n{body}\n{pem_parts[3]}".encode()
