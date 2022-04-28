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

### Write and run a flow

```python
from prefect import flow
from prefect_snowflake import SnowflakeCredentials
from prefect_snowflake.database import snowflake_query


@flow
def snowflake_query_flow():
    snowflake_credentials = SnowflakeCredentials(
        account="account",
        user="user",
        password="password",
    )
    result = snowflake_query(
        "SELECT * FROM table WHERE id=%{id_param}s LIMIT 8;",
        snowflake_credentials,
        params={"id_param": 1}
    )
    return result

snowflake_query_flow()
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
