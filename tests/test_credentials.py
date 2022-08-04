from unittest.mock import MagicMock

import pytest
from pydantic import SecretStr

from prefect_snowflake.credentials import SnowflakeCredentials


@pytest.fixture()
def connect_params():
    return {
        "account": "account",
        "user": "user",
        "password": "password",
        "database": "database",
        "warehouse": "warehouse",
    }


@pytest.fixture
def snowflake_auth(monkeypatch):
    auth_mock = MagicMock()
    auth_mock.authenticate.side_effect = lambda: "authenticated"
    monkeypatch.setattr("snowflake.connector.connection.Auth", auth_mock)


def test_snowflake_credentials_get_connect_params(connect_params):
    snowflake_credentials = SnowflakeCredentials(**connect_params)
    actual_connect_params = snowflake_credentials._get_connect_params()
    for param in connect_params:
        actual = actual_connect_params[param]
        expected = connect_params[param]
        if isinstance(actual, SecretStr):
            actual = actual.get_secret_value()
        assert actual == expected

    valid_params = dir(SnowflakeCredentials)
    for param in valid_params:
        if param.startswith("_"):
            continue

        expected = getattr(snowflake_credentials, param)
        if callable(expected):
            continue

        if expected is None:
            # it is filtered out
            assert param not in snowflake_credentials.connect_params
        else:
            # the value is correct
            assert snowflake_credentials.connect_params[param] == expected


def test_snowflake_credentials_validate_auth_kwargs(snowflake_auth, connect_params):
    connect_params_missing = connect_params.copy()
    connect_params_missing.pop("password")
    with pytest.raises(ValueError, match="One of the authentication keys"):
        SnowflakeCredentials(**connect_params_missing)


def test_snowflake_credentials_get_connect_params_override(
    snowflake_auth, connect_params
):
    snowflake_credentials = SnowflakeCredentials(**connect_params)
    new_connect_params = snowflake_credentials._get_connect_params(
        database="override_database", warehouse="override_warehouse"
    )

    for param in ["database", "warehouse"]:
        actual = new_connect_params[param]
        expected = "override_" + connect_params[param]
        assert actual == expected


def test_snowflake_credentials_get_connect_params_get_secret_value(
    snowflake_auth, connect_params
):
    snowflake_credentials = SnowflakeCredentials(**connect_params)
    connect_params = snowflake_credentials._get_connect_params()
    assert connect_params["password"] == "password"


@pytest.mark.parametrize("param", ("database", "warehouse"))
def test_snowflake_credentials_get_connect_params_missing_input(
    snowflake_auth, connect_params, param
):
    connect_params_missing = connect_params.copy()
    connect_params_missing.pop(param)
    with pytest.raises(ValueError, match=f"The {param} must be set in either"):
        SnowflakeCredentials(**connect_params_missing)._get_connect_params()


def test_snowflake_password_is_secret_str():
    password = "dontshowthis"
    credentials = SnowflakeCredentials(
        account="name_of_account", user="user", password=password
    )
    connect_params_password = credentials.password
    assert isinstance(connect_params_password, SecretStr)
    assert connect_params_password.get_secret_value() == password
