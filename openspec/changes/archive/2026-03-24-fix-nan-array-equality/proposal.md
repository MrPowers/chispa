## Why

`assert_df_equality(..., allow_nan_equality=True)` currently fails when compared rows contain array values with `NaN` in the same positions. This violates the documented intent of `allow_nan_equality` and causes false negatives for valid equality checks.

## What Changes

- Define and enforce `allow_nan_equality=True` behavior for array-valued fields in `assert_df_equality` so that `NaN` values are considered equal when they appear at the same array index.
- Preserve current behavior for all existing paths that do not require this fix.
- Add regression tests that lock expected outcomes for array `NaN` equality.
- Explicitly out of scope: API signature changes, default behavior changes, approximate-equality behavior changes, and non-`assert_df_equality` feature additions.

## Capabilities

### New Capabilities

- `nan-array-equality`: `assert_df_equality` with `allow_nan_equality=True` treats array fields as equal when both arrays have `NaN` at identical positions; it still fails for different positions, different lengths, or non-`NaN` value mismatches.

### Modified Capabilities

- `allow-nan-equality`: expands from top-level scalar `NaN` handling to include array-valued fields under `assert_df_equality`, while keeping `allow_nan_equality=False` semantics unchanged.

## Impact

- Affected behavior is narrowly scoped to `assert_df_equality` with `allow_nan_equality=True` and array-valued cells.
- Backward compatibility is preserved: no public API changes, no new flags, no changed defaults.
- Regression risk is controlled by explicit tests for equal arrays with aligned `NaN`, unequal arrays with misaligned `NaN`, and unchanged behavior when `allow_nan_equality=False`.
