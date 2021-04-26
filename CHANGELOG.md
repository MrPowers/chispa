# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

#### Changed

* `DataFramesNotEqualError` changed to `RowsNotEqualError` to reflect it being raised when testing for row equality.

#### Removed

* Removed `are_dfs_equal` because it has been superseded by other parts of the API.
* Removed `assert_approx_df_equality` as it has been replaced by adding the optional `precision` parameter to `assert_df_equality`.

