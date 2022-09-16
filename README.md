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
    <a href="https://prefect-snowflake-community.slack.com" alt="Slack">
        <img src="https://img.shields.io/badge/slack-join_community-red.svg?color=0052FF&labelColor=090422&logo=slack" /></a>
    <a href="https://discourse.prefect-snowflake.io/" alt="Discourse">
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

Then, register to [view the block](https://orion-docs.prefect.io/ui/blocks/) on Prefect Cloud:

```bash
prefect block register -m prefect_snowflake.credentials
```

Note, to use the `load` method on Blocks, you must already have a block document [saved through code](https://orion-docs.prefect.io/concepts/blocks/#saving-blocks) or [saved through the UI](https://orion-docs.prefect.io/ui/blocks/).

### Write and run a flow

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

## Resources

If you encounter any bugs while using `prefect-snowflake`, feel free to open an issue in the [prefect-snowflake](https://github.com/PrefectHQ/prefect-snowflake) repository.

If you have any questions or issues while using `prefect-snowflake`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

Feel free to ⭐️ or watch [`prefect-snowflake`](https://github.com/PrefectHQ/prefect-snowflake) for updates too!

## Development

If you'd like to install a version of `prefect-snowflake` for development, clone the repository and perform an editable install with `pip`:

```bash
git clone https://github.com/PrefectHQ/prefect-snowflake.git

cd prefect-snowflake/

pip install -e ".[dev]"

# Install linting pre-commit hooks
pre-commit install
```
