from . import _version
from prefect_snowflake.credentials import (  # noqa
    SnowflakeCredentials,
)

__version__ = _version.get_versions()["version"]
