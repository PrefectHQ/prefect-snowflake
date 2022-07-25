from unittest.mock import MagicMock

import pytest

from prefect_snowflake.credentials import SnowflakeCredentials


@pytest.fixture()
def connection_params():
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


def test_snowflake_credentials_post_init(connection_params):
    snowflake_credentials = SnowflakeCredentials(**connection_params)
    for param in connection_params:
        expected = connection_params[param]
        actual = getattr(snowflake_credentials, param)
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


def test_snowflake_credentials_get_connection_override(
    snowflake_auth, connection_params
):
    connection = SnowflakeCredentials(**connection_params).get_connection(
        database="override_database", warehouse="override_warehouse"
    )

    for param in connection_params:
        if param == "password":
            continue
        expected = connection_params[param]
        if param in ["database", "warehouse"]:
            expected = f"override_{expected}"
        assert getattr(connection, param) == expected


def test_snowflake_credentials_get_connection_missing_auth(
    snowflake_auth, connection_params
):
    connection_params_missing = connection_params.copy()
    connection_params_missing.pop("password")
    with pytest.raises(ValueError, match="One of the authentication methods"):
        SnowflakeCredentials(**connection_params_missing).get_connection()


@pytest.mark.parametrize("param", ("database", "warehouse"))
def test_snowflake_credentials_get_connection_missing_input(
    snowflake_auth, connection_params, param
):
    connection_params_missing = connection_params.copy()
    connection_params_missing.pop(param)
    with pytest.raises(ValueError, match=f"The {param} must be set in either"):
        SnowflakeCredentials(**connection_params_missing).get_connection()
