# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Changed

- Added `get_client` method to `SnowflakeCredentials` to enable more customization of connection creation - [#51](https://github.com/PrefectHQ/prefect-snowflake/pull/51)

### Deprecated

### Removed

### Fixed

### Security

## 0.2.3

Released on December 21st, 2022.

### Deprecated

- The `okta_endpoint` field in `SnowflakeCredentials`; use `endpoint` instead - [#45](https://github.com/PrefectHQ/prefect-snowflake/pull/45).

### Fixed

- Fixed misleading validator message in `SnowflakeCredentials` when `authenticator` is `okta_endpoint` - [#45](https://github.com/PrefectHQ/prefect-snowflake/pull/45).

## 0.2.2

Released on October 5th, 2022.

### Added
- `snowflake_query_sync` [#34](https://github.com/PrefectHQ/prefect-snowflake/pull/34).

## 0.2.1

Released on August 22nd, 2022.

### Added
- `poll_frequency_seconds` to `snowflake_query` and `snowflake_multiquery` [#29](https://github.com/PrefectHQ/prefect-snowflake/pull/29).

## 0.2.0
Released on August 15th, 2022.

Note, with this release, the `database`, `warehouse` and `schema` fields from `SnowflakeCredentials` have been migrated to `SnowflakeConnector`:
```
from prefect_snowflake.credentials import SnowflakeCredentials
from prefect_snowflake.database import SnowflakeConnector
...
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
```

Tasks now accept `SnowflakeConnector` instead of `SnowflakeCredentials`:
```
snowflake_query(..., snowflake_connector)
```

### Added
- `SnowflakeConnector` block - [#24](https://github.com/PrefectHQ/prefect-snowflake/pull/24)
- `okta_endpoint` field to `SnowflakeCredentials` - [#25](https://github.com/PrefectHQ/prefect-snowflake/pull/25)

### Changed
- Moved the keywords, `database`, `warehouse`, and `schema` from `credentials.SnowflakeCredentials` into `database.SnowflakeConnector` - [#24](https://github.com/PrefectHQ/prefect-snowflake/pull/24)
- Moved the method `get_connection` from `credentials.SnowflakeCredentials` into `database.SnowflakeConnector` - [#24](https://github.com/PrefectHQ/prefect-snowflake/pull/24)
- `authenticator` field in `SnowflakeCredentials` to `Literal` type - [#25](https://github.com/PrefectHQ/prefect-snowflake/pull/25)

### Removed
- Removed the keywords, `database` and `warehouse`, from `snowflake_query` and `snowflake_multiquery` - [#24](https://github.com/PrefectHQ/prefect-snowflake/pull/24)

### Security
- Fixed revealing the input password nested under `connect_params` when logging `SnowflakeCredentials` - [#24](https://github.com/PrefectHQ/prefect-snowflake/pull/24)

## 0.1.3
Released on July 26th, 2022.

### Fixed
- Fixed credentials by calling `get_secret_value()` on `SecretStr` keywords - [#19](https://github.com/PrefectHQ/prefect-snowflake/pull/19)

## 0.1.2
Released on July 22nd, 2022.

### Added
- Added setup.py entry point - [#18](https://github.com/PrefectHQ/prefect-snowflake/pull/18)

## 0.1.1

Released on July 19th, 2022.

### Added
- Support for running multiple queries in a one session and in a transaction - [#9](https://github.com/PrefectHQ/prefect-snowflake/pull/9)

### Changed
- Converted `SnowflakeCredentials` into a `Block` - [#13](https://github.com/PrefectHQ/prefect-snowflake/pull/13).
- Updated tests to be compatible with core Prefect library (v2.0b9) and bumped required version - [#14](https://github.com/PrefectHQ/prefect-snowflake/pull/14).

## 0.1.0

Released on May 13th, 2022.

### Added

- `snowflake_query` task - [#5](https://github.com/PrefectHQ/prefect-snowflake/pull/5)
