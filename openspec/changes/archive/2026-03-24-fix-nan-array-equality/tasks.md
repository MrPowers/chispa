## 1. NaN-safe row comparison update

- [x] 1.1 Add deep NaN-aware value comparison for row field values used by `allow_nan_equality=True`.
- [x] 1.2 Update `are_rows_equal_enhanced` to use deep comparison only in the `allow_nan_equality=True` path.
- [x] 1.3 Verify `allow_nan_equality=False` path remains unchanged and retains strict existing behavior.

## 2. Regression tests for array NaN equality

- [x] 2.1 Add `assert_df_equality` test where array fields with `NaN` at identical indexes pass with `allow_nan_equality=True`.
- [x] 2.2 Add `assert_df_equality` test where array fields with misaligned `NaN` indexes fail with `allow_nan_equality=True`.
- [x] 2.3 Add `assert_df_equality` test where array fields with matching `NaN` still fail when `allow_nan_equality=False`.

## 3. Focused low-level coverage and validation

- [x] 3.1 Add row comparer unit test coverage for nested array value comparisons involving `NaN`.
- [x] 3.2 Run targeted NaN-related tests for DataFrame comparer and row comparer.
- [ ] 3.3 Run full project checks (`make check`) to confirm no regressions.
