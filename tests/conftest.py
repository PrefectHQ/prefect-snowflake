import pytest

from prefect_snowflake.credentials import SnowflakeCredentials


@pytest.fixture()
def credentials_params():
    return {
        "account": "account",
        "user": "user",
        "password": "password",
    }


@pytest.fixture()
def connector_params(credentials_params):
    snowflake_credentials = SnowflakeCredentials(**credentials_params)
    _connector_params = {
        "schema": "schema_input",
        "database": "database",
        "warehouse": "warehouse",
        "credentials": snowflake_credentials,
    }
    return _connector_params
