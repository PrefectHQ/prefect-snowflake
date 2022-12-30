import os
from unittest.mock import MagicMock

import pytest
from prefect.testing.utilities import prefect_test_harness

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


@pytest.fixture(scope="session", autouse=True)
def prefect_db():
    """
    Sets up test harness for temporary DB during test runs.
    """
    with prefect_test_harness():
        yield


@pytest.fixture(autouse=True)
def reset_object_registry():
    """
    Ensures each test has a clean object registry.
    """
    from prefect.context import PrefectObjectRegistry

    with PrefectObjectRegistry():
        yield


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


@pytest.fixture
def snowflake_connect_mock(monkeypatch):
    connect_mock = MagicMock()
    monkeypatch.setattr("snowflake.connector.connect", connect_mock)
    return connect_mock
