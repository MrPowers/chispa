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

### Requirement: README documents allow_nan_equality behavior consistently
The project README SHALL describe `allow_nan_equality` behavior for DataFrame equality in a way that is consistent with the existing `allow-nan-equality` capability semantics.

#### Scenario: README describes opt-in NaN equality
- **WHEN** README explains NaN comparison behavior
- **THEN** it SHALL state that NaN equality is enabled only when `allow_nan_equality=True`

#### Scenario: README preserves strict default semantics
- **WHEN** README explains default DataFrame equality behavior
- **THEN** it SHALL indicate that NaN values are not treated as equal by default

#### Scenario: README example does not redefine behavior
- **WHEN** README contains code examples for NaN handling
- **THEN** the examples SHALL demonstrate existing behavior without introducing new semantics or flags
