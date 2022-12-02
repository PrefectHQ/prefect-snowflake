import os

import pytest

from prefect_snowflake.credentials import SnowflakeCredentials


def _read_test_file(name: str) -> bytes:
    """
    Args:
        name: File to load from test_data folder.

    Returns:
        File content as binary.
    """
    full_name = os.path.join(os.path.split(__file__)[0], "test_data", name)
    with open(full_name, "rb") as fd:
        return fd.read()


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
    return {
        "account": "account",
        "user": "user",
        "password": "letmein",
        "private_key": _read_test_file("test_cert.p8"),
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
    return {
        "account": "account",
        "user": "user",
        "password": "letmein",
        "private_key": _read_test_file("test_cert_no_pass.p8"),
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
    return {
        "account": "account",
        "user": "user",
        "password": "letmein",
        "private_key": _read_test_file("test_cert_malformed_format.p8"),
    }
