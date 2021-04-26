# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Changed
- `DataFramesNotEqualError` changed to `RowsNotEqualError` to reflect it being raised when testing for row equality.
- The assertion functions `assert_df_equality` and `assert_column_equality` now have optional `precision` parameter to test for approximate equality.

### Removed
- Removed `are_dfs_equal` because it has been superseded by other parts of the API.
- Removed `assert_approx_df_equality` as it has been replaced by adding the optional `precision` parameter to `assert_df_equality`.
- Removed `assert_approx_column_equality` as it has been replaced by adding the optional `precision` parameter to `assert_column_equality`.
