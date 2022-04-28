from . import _version
from prefect_snowflake.credentials import SnowflakeCredentials  # noqa

__version__ = _version.get_versions()["version"]
