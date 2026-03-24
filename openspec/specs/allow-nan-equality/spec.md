## Purpose
Define how `assert_df_equality` evaluates `NaN` values with `allow_nan_equality`, including scalar and array-valued fields.

## Requirements

### Requirement: allow_nan_equality behavior in assert_df_equality
`assert_df_equality` SHALL consider `NaN` values equal when `allow_nan_equality=True`. This SHALL apply to top-level scalar fields and to array-valued fields, while preserving existing strict equality behavior when `allow_nan_equality=False`.

#### Scenario: Top-level scalar NaN values
- **WHEN** two compared DataFrames have `NaN` in the same top-level scalar field and `allow_nan_equality=True`
- **THEN** `assert_df_equality` SHALL treat the fields as equal

#### Scenario: Array-valued fields with NaN values
- **WHEN** two compared DataFrames have array-valued fields with `NaN` at identical indexes and `allow_nan_equality=True`
- **THEN** `assert_df_equality` SHALL treat the fields as equal

#### Scenario: Default behavior remains strict
- **WHEN** two compared DataFrames have `NaN` values in corresponding fields and `allow_nan_equality=False`
- **THEN** `assert_df_equality` SHALL raise `DataFramesNotEqualError` under existing strict equality semantics
