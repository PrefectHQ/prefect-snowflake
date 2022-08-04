# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

### Changed

### Deprecated

### Removed

### Fixed
- Keeping `password`, `token`, and `private_key` as `SecretStr` in `block_initialization` so logging doesn't show secrets - [#22](https://github.com/PrefectHQ/prefect-snowflake/pull/22)

### Security

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
