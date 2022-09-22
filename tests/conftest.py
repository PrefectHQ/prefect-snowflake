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


@pytest.fixture()
def private_credentials_params():
    import os

    cert_path = os.path.join(os.path.split(__file__)[0], "test_cert.p8")
    with open(cert_path, "rb") as fd:
        return {
            "account": "account",
            "user": "user",
            "password": "letmein",
            "private_key": fd.read(),
        }


@pytest.fixture()
def private_connector_params(private_credentials_params):

    snowflake_credentials = SnowflakeCredentials(**private_credentials_params)
    _connector_params = {
        "schema": "schema_input",
        "database": "database",
        "warehouse": "warehouse",
        "credentials": snowflake_credentials,
    }
    return _connector_params


@pytest.fixture()
def private_no_pass_credentials_params():
    import os

    cert_path = os.path.join(os.path.split(__file__)[0], "test_cert_no_pass.p8")
    with open(cert_path, "rb") as fd:
        return {
            "account": "account",
            "user": "user",
            "password": "letmein",
            "private_key": fd.read(),
        }


@pytest.fixture()
def private_no_pass_connector_params(private_no_pass_credentials_params):

    snowflake_credentials = SnowflakeCredentials(**private_no_pass_credentials_params)
    _connector_params = {
        "schema": "schema_input",
        "database": "database",
        "warehouse": "warehouse",
        "credentials": snowflake_credentials,
    }
    return _connector_params


@pytest.fixture()
def private_malformed_credentials_params():
    import os

    cert_path = os.path.join(
        os.path.split(__file__)[0], "test_cert_malformed_format.p8"
    )
    with open(cert_path, "rb") as fd:
        return {
            "account": "account",
            "user": "user",
            "password": "letmein",
            "private_key": fd.read(),
        }
