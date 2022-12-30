# prefect-snowflake

<p align="center">
    <a href="https://pypi.python.org/pypi/prefect-snowflake/" alt="PyPI version">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/prefect-snowflake?color=0052FF&labelColor=090422"></a>
    <a href="https://github.com/PrefectHQ/prefect-snowflake/" alt="Stars">
        <img src="https://img.shields.io/github/stars/PrefectHQ/prefect-snowflake?color=0052FF&labelColor=090422" /></a>
    <a href="https://pepy.tech/badge/prefect-snowflake/" alt="Downloads">
        <img src="https://img.shields.io/pypi/dm/prefect-snowflake?color=0052FF&labelColor=090422" /></a>
    <a href="https://github.com/PrefectHQ/prefect-snowflake/pulse" alt="Activity">
        <img src="https://img.shields.io/github/commit-activity/m/PrefectHQ/prefect-snowflake?color=0052FF&labelColor=090422" /></a>
    <br>
    <a href="https://prefect-community.slack.com" alt="Slack">
        <img src="https://img.shields.io/badge/slack-join_community-red.svg?color=0052FF&labelColor=090422&logo=slack" /></a>
    <a href="https://discourse.prefect.io/" alt="Discourse">
        <img src="https://img.shields.io/badge/discourse-browse_forum-red.svg?color=0052FF&labelColor=090422&logo=discourse" /></a>
</p>

## Welcome!

Prefect integrations for interacting with prefect-snowflake.

## Getting Started

### Python setup

Requires an installation of Python 3.7+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://orion-docs.prefect.io/).

### Installation

Install `prefect-snowflake` with `pip`:

```bash
pip install prefect-snowflake
```

A list of available blocks in `prefect-snowflake` and their setup instructions can be found [here](https://PrefectHQ.github.io/prefect-snowflake/#blocks-catalog).

### Query from table

```python
from prefect import flow
from prefect_snowflake.credentials import SnowflakeCredentials
from prefect_snowflake.database import SnowflakeConnector, snowflake_query


@flow
def snowflake_query_flow():
    snowflake_credentials = SnowflakeCredentials(
        account="account",
        user="user",
        password="password",
    )
    snowflake_connector = SnowflakeConnector(
        database="database",
        warehouse="warehouse",
        schema="schema",
        credentials=snowflake_credentials
    )
    result = snowflake_query(
        "SELECT * FROM table WHERE id=%{id_param}s LIMIT 8;",
        snowflake_connector,
        params={"id_param": 1}
    )
    return result

snowflake_query_flow()
```

### Write pandas to table using block attributes

```python
import pandas as pd
from prefect import flow
from prefect_snowflake.credentials import SnowflakeCredentials
from prefect_snowflake.database import SnowflakeConnector, snowflake_query
from snowflake.connector.pandas_tools import write_pandas

@flow
def snowflake_write_pandas_flow():
    snowflake_connector = SnowflakeConnector.load("my-block")
    with snowflake_connector.get_connection() as conn:
        table_name = "TABLE_NAME"
        ddl = "NAME STRING, NUMBER INT"
        statement = f'CREATE TABLE IF NOT EXISTS {table_name} ({ddl})'
        with conn.cursor() as cur:
            cur.execute(statement)

        # case sensitivity matters here!
        df = pd.DataFrame([('Marvin', 42), ('Ford', 88)], columns=['NAME', 'NUMBER'])
        success, num_chunks, num_rows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            database=snowflake_connector.database,
            schema=snowflake_connector.schema_  # note the "_" suffix
        )
```

### Execute `get` and `put` statements

To execute `get` and `put` statements, use `snowflake_query_sync`.

```python
from prefect import flow
from prefect_snowflake.database import SnowflakeConnector, snowflake_query_sync

@flow
def snowflake_put_file_to_snowflake_stage():
    snowflake_connector = SnowflakeConnector.load("my-block")
    
    snowflake_query_sync(
        f"put file:///myfolder/myfile @mystage/mystagepath",
        snowflake_connector=snowflake_connector
    )
            
```

### Use `with_options` to customize options on any existing task or flow:

```python
from prefect import flow
from prefect_snowflake.database import SnowflakeConnector, snowflake_query_sync

custom_snowflake_query_sync = snowflake_query_sync.with_options(
    name="My custom task name",
    retries=2,
    retry_delay_seconds=10,
)

@flow
def example_with_options_flow():
snowflake_connector = SnowflakeConnector.load("my-block")

custom_snowflake_query_sync(
    f"put file:///myfolder/myfile @mystage/mystagepath",
    snowflake_connector=snowflake_connector
)

example_with_options_flow()
```
 
For more tips on how to use tasks and flows in a Collection, check out [Using Collections](https://orion-docs.prefect.io/collections/usage/)!

## Resources

If you encounter any bugs while using `prefect-snowflake`, feel free to open an issue in the [prefect-snowflake](https://github.com/PrefectHQ/prefect-snowflake) repository.

If you have any questions or issues while using `prefect-snowflake`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

Feel free to star or watch [`prefect-snowflake`](https://github.com/PrefectHQ/prefect-snowflake) for updates too!

## Contribute

If you'd like to help contribute to fix an issue or add a feature to `prefect-snowflake`, please [propose changes through a pull request from a fork of the repository](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).

### Contribution Steps:
1. [Fork the repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#forking-a-repository)
2. [Clone the forked repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#cloning-your-forked-repository)
3. Install the repository and its dependencies:
```
pip install -e ".[dev]"
```
4. Make desired changes.
5. Add tests.
6. Insert an entry to [CHANGELOG.md](https://github.com/PrefectHQ/prefect-snowflake/blob/main/CHANGELOG.md)
7. Install `pre-commit` to perform quality checks prior to commit:
```
pre-commit install
```
8. `git commit`, `git push`, and create a pull request.
