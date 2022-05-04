from unittest.mock import MagicMock

import pytest
from prefect import flow

from prefect_snowflake.database import snowflake_query


class SnowflakeCursor:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params):
        resp = MagicMock()
        resp.fetchall.return_value = [(query, params),]
        return resp


class SnowflakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self, cursor_type):
        return SnowflakeCursor()


@pytest.fixture()
def snowflake_credentials():
    snowflake_credentials_mock = MagicMock()
    snowflake_credentials_mock.get_connection.return_value = SnowflakeConnection()
    return snowflake_credentials_mock


def test_snowflake_query(snowflake_credentials):
    @flow
    def test_flow():
        result = snowflake_query(
            "query",
            snowflake_credentials,
            params=("param",),
        )
        return result

    result = test_flow().result().result()
    assert result[0][0] == "query"
    assert result[0][1] == ("param",)
