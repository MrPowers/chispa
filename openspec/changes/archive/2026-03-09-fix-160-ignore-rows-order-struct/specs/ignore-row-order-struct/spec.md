## ADDED Requirements

### Requirement: ignore_row_order supports struct types
The system SHALL allow `ignore_row_order=True` to work with struct-typed columns by applying a hash function before sorting, avoiding the `DATATYPE_MISMATCH.INVALID_ORDERING_TYPE` error.

#### Scenario: DataFrame with struct column and ignore_row_order=True
- **WHEN** a user calls `assert_df_equality` with `ignore_row_order=True` on DataFrames containing struct-typed columns
- **THEN** the system applies `pyspark.functions.hash` to struct columns before sorting and no `AnalysisException` is raised

#### Scenario: DataFrame with mixed column types and ignore_row_order=True
- **WHEN** a user calls `assert_df_equality` with `ignore_row_order=True` on DataFrames containing both struct and non-struct columns
- **THEN** the system applies `pyspark.functions.hash` only to struct columns and sorts non-struct columns normally

#### Scenario: DataFrame with nested struct column and ignore_row_order=True
- **WHEN** a user calls `assert_df_equality` with `ignore_row_order=True` on DataFrames containing nested struct-typed columns
- **THEN** the system applies `pyspark.functions.hash` to the nested struct columns and no `AnalysisException` is raised
