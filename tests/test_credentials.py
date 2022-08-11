from unittest.mock import MagicMock

import pytest
from pydantic import SecretStr

from prefect_snowflake.credentials import SnowflakeConnector, SnowflakeCredentials


@pytest.fixture()
def credentials_params():
    return {
        "account": "account",
        "user": "user",
        "password": "password",
    }


@pytest.fixture()
def connector_params(credentials_params):
    _connector_params = credentials_params.copy()
    _connector_params.update(
        {
            "database": "database",
            "warehouse": "warehouse",
        }
    )
    return _connector_params


@pytest.fixture
def snowflake_auth(monkeypatch):
    auth_mock = MagicMock()
    auth_mock.authenticate.side_effect = lambda: "authenticated"
    monkeypatch.setattr("snowflake.connector.connection.Auth", auth_mock)


def test_snowflake_credentials_init(credentials_params):
    snowflake_credentials = SnowflakeCredentials(**credentials_params)
    actual_credentials_params = snowflake_credentials.dict()
    for param in credentials_params:
        actual = actual_credentials_params[param]
        expected = credentials_params[param]
        if isinstance(actual, SecretStr):
            actual = actual.get_secret_value()
        assert actual == expected


def test_snowflake_credentials_validate_auth_kwargs(credentials_params):
    credentials_params_missing = credentials_params.copy()
    credentials_params_missing.pop("password")
    with pytest.raises(ValueError, match="One of the authentication keys"):
        SnowflakeCredentials(**credentials_params_missing)


def test_snowflake_connector_init(connector_params):
    snowflake_credentials = SnowflakeConnector(**connector_params)
    actual_connector_params = snowflake_credentials.dict()
    for param in connector_params:
        actual = actual_connector_params[param]
        expected = connector_params[param]
        if isinstance(actual, SecretStr):
            actual = actual.get_secret_value()
        assert actual == expected


def test_snowflake_connector_password_is_secret_str(connector_params):
    snowflake_credentials = SnowflakeConnector(**connector_params)
    password = snowflake_credentials.password
    assert isinstance(password, SecretStr)
    assert password.get_secret_value() == "password"


def test_snowflake_connector_get_connect_params_get_secret_value(connector_params):
    snowflake_credentials = SnowflakeConnector(**connector_params)
    connector_params = snowflake_credentials._get_connect_params()
    assert connector_params["password"] == "password"
