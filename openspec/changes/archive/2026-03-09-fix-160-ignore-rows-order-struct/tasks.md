## 1. Implementation

- [x] 1.1 Add helper function to detect struct-type columns in a DataFrame schema
- [x] 1.2 Modify the `ignore_row_order` transform in `assert_df_equality` to apply `hash()` to struct columns before sorting
- [x] 1.3 Apply the same fix to `assert_approx_df_equality` function
- [x] 1.4 Import `pyspark.sql.functions` for the `hash` function

## 2. Testing

- [x] 2.1 Create test case for DataFrame with struct column and `ignore_row_order=True`
- [x] 2.2 Create test case for DataFrame with mixed column types (struct + primitive) and `ignore_row_order=True`
- [x] 2.3 Create test case for DataFrame with nested struct column and `ignore_row_order=True`
- [x] 2.4 Verify existing tests still pass (no regression for primitive types)

## 3. Verification

- [x] 3.1 Run the full test suite to ensure no regressions
- [x] 3.2 Manually test the fix with the original error scenario from the proposal
