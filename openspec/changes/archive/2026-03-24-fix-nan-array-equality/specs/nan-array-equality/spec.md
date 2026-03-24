## ADDED Requirements

### Requirement: Array fields honor allow_nan_equality in assert_df_equality
When `assert_df_equality` is called with `allow_nan_equality=True`, the system SHALL treat two array-valued fields as equal when both arrays have `NaN` values at the same indexes.

#### Scenario: Matching NaN positions in arrays
- **WHEN** two compared DataFrames contain array fields with equal lengths and `NaN` values at identical indexes
- **THEN** `assert_df_equality(..., allow_nan_equality=True)` SHALL treat those array fields as equal

#### Scenario: Different NaN positions in arrays
- **WHEN** two compared DataFrames contain array fields where `NaN` appears at different indexes
- **THEN** `assert_df_equality(..., allow_nan_equality=True)` SHALL raise `DataFramesNotEqualError`

#### Scenario: Different array lengths
- **WHEN** two compared DataFrames contain array fields with different lengths
- **THEN** `assert_df_equality(..., allow_nan_equality=True)` SHALL raise `DataFramesNotEqualError`

#### Scenario: Non-NaN element mismatch
- **WHEN** two compared DataFrames contain array fields with at least one differing non-`NaN` element value at the same index
- **THEN** `assert_df_equality(..., allow_nan_equality=True)` SHALL raise `DataFramesNotEqualError`
