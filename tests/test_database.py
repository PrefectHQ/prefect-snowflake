from unittest.mock import MagicMock

import pytest
from prefect import flow
from pydantic import SecretBytes, SecretStr

from prefect_snowflake.database import (
    BEGIN_TRANSACTION_STATEMENT,
    END_TRANSACTION_STATEMENT,
    SnowflakeConnector,
    snowflake_multiquery,
    snowflake_query,
    snowflake_query_sync,
)


def test_snowflake_connector_init(connector_params):
    snowflake_connector = SnowflakeConnector(**connector_params)
    actual_connector_params = snowflake_connector.dict()
    for param in connector_params:
        expected = connector_params[param]
        if param == "schema":
            param = "schema_"
        actual = actual_connector_params[param]
        if isinstance(actual, SecretStr):
            actual = actual.get_secret_value()
        assert actual == expected


def test_snowflake_connector_password_is_secret_str(connector_params):
    snowflake_connector = SnowflakeConnector(**connector_params)
    password = snowflake_connector.credentials.password
    assert isinstance(password, SecretStr)
    assert password.get_secret_value() == "password"


class SnowflakeCursor:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute_async(self, query, params):
        query_id = "1234"
        self.result = {query_id: [(query, params)]}
        return {"queryId": query_id}

    def get_results_from_sfqid(self, query_id):
        self.query_result = self.result[query_id]

    def fetchall(self):
        return self.query_result

    def execute(self, query, params=None):
        self.query_result = [(query, params, "sync")]
        return self


class SnowflakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self, cursor_type):
        return SnowflakeCursor()

    def is_still_running(self, state):
        return state

    def get_query_status_throw_if_error(self, query_id):
        return False


@pytest.fixture()
def snowflake_connector():
    snowflake_connector_mock = MagicMock()
    snowflake_connector_mock.get_connection.return_value = SnowflakeConnection()
    return snowflake_connector_mock


def test_snowflake_query(snowflake_connector):
    @flow
    def test_flow():
        result = snowflake_query(
            "query",
            snowflake_connector,
            params=("param",),
        )
        return result

    result = test_flow()
    assert result[0][0] == "query"
    assert result[0][1] == ("param",)


def test_snowflake_multiquery(snowflake_connector):
    @flow
    def test_flow():
        result = snowflake_multiquery(
            ["query1", "query2"],
            snowflake_connector,
            params=("param",),
        )
        return result

    result = test_flow()
    assert result[0][0][0] == "query1"
    assert result[0][0][1] == ("param",)
    assert result[1][0][0] == "query2"
    assert result[1][0][1] == ("param",)


def test_snowflake_multiquery_transaction(snowflake_connector):
    @flow
    def test_flow():
        result = snowflake_multiquery(
            ["query1", "query2"],
            snowflake_connector,
            params=("param",),
            as_transaction=True,
        )
        return result

    result = test_flow()
    assert result[0][0][0] == "query1"
    assert result[0][0][1] == ("param",)
    assert result[1][0][0] == "query2"
    assert result[1][0][1] == ("param",)


def test_snowflake_multiquery_transaction_with_transaction_control_results(
    snowflake_connector,
):
    @flow
    def test_flow():
        result = snowflake_multiquery(
            ["query1", "query2"],
            snowflake_connector,
            params=("param",),
            as_transaction=True,
            return_transaction_control_results=True,
        )
        return result

    result = test_flow()
    assert result[0][0][0] == BEGIN_TRANSACTION_STATEMENT
    assert result[1][0][0] == "query1"
    assert result[1][0][1] == ("param",)
    assert result[2][0][0] == "query2"
    assert result[2][0][1] == ("param",)
    assert result[3][0][0] == END_TRANSACTION_STATEMENT


def test_snowflake_query_sync(snowflake_connector):
    @flow()
    def test_snowflake_query_sync_flow():
        result = snowflake_query_sync("query", snowflake_connector, params=("param",))
        return result

    result = test_snowflake_query_sync_flow()
    assert result[0][0] == "query"
    assert result[0][1] == ("param",)
    assert result[0][2] == "sync"


def test_snowflake_private_connector_init(private_connector_params):
    snowflake_connector = SnowflakeConnector(**private_connector_params)
    actual_connector_params = snowflake_connector.dict()
    for param in private_connector_params:
        expected = private_connector_params[param]
        if param == "schema":
            param = "schema_"
        actual = actual_connector_params[param]
        if isinstance(actual, (SecretStr, SecretBytes)):
            actual = actual.get_secret_value()
        assert actual == expected


def test_snowflake_connector_private_key_is_secret(private_connector_params):
    snowflake_connector = SnowflakeConnector(**private_connector_params)
    private_key = snowflake_connector.credentials.private_key
    assert isinstance(private_key, (SecretStr, SecretBytes))


def test_snowflake_connector_unencrypted_private_key_no_password(
    private_no_pass_connector_params,
):
    snowflake_connector = SnowflakeConnector(**private_no_pass_connector_params)
    snowflake_connector.credentials.password = None
    assert snowflake_connector.credentials.private_key is not None
    # Raises error if invalid
    snowflake_connector._get_connect_params()


def test_snowflake_connector_unencrypted_private_key_empty_password(
    private_no_pass_connector_params,
):
    snowflake_connector = SnowflakeConnector(**private_no_pass_connector_params)
    assert snowflake_connector.credentials.private_key is not None

    snowflake_connector.credentials.password = SecretBytes(b" ")
    snowflake_connector._get_connect_params()
    snowflake_connector.credentials.password = SecretBytes(b"")
    snowflake_connector._get_connect_params()
    snowflake_connector.credentials.password = SecretStr("")
    snowflake_connector._get_connect_params()
    snowflake_connector.credentials.password = SecretStr("   ")
    snowflake_connector._get_connect_params()




def test_snowflake_connector_encrypted_private_key_is_valid(private_connector_params):
    snowflake_connector = SnowflakeConnector(**private_connector_params)
    assert snowflake_connector.credentials.private_key is not None
    assert snowflake_connector.credentials.password is not None
    # Raises error if invalid
    snowflake_connector._get_connect_params()

