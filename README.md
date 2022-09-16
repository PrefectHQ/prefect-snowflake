# prefect-snowflake

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

## Resources

If you encounter any bugs while using `prefect-snowflake`, feel free to open an issue in the [prefect-snowflake](https://github.com/PrefectHQ/prefect-snowflake) repository.

If you have any questions or issues while using `prefect-snowflake`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

## Development

If you'd like to install a version of `prefect-snowflake` for development, clone the repository and perform an editable install with `pip`:

```bash
git clone https://github.com/PrefectHQ/prefect-snowflake.git

cd prefect-snowflake/

pip install -e ".[dev]"

# Install linting pre-commit hooks
pre-commit install
```
