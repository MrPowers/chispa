## Why

There is insufficient test coverage for the combination of `ignore_nullable=True` AND `ignore_metadata=True` flags when comparing schemas containing array types. This gap in coverage could allow regressions in the `are_datatypes_equal_ignore_nullable` function to go undetected.

## What Changes

- Add test cases for `ignore_nullable=True` AND `ignore_metadata=True` combination with:
  - Array of primitive types with different nullable flags
  - Array of structs with different metadata
  - Nested arrays (Array of Array) with different metadata
  - Array of structs containing fields with different metadata
- Verify the existing behavior for these combinations

## Capabilities

### New Capabilities
- `ignore-nullable-metadata-array`: Tests for schema comparison when both `ignore_nullable` and `ignore_metadata` are `True` with array types

### Modified Capabilities
- (none - this is pure test coverage, no behavior change)

## Impact

- **Affected code**: `chispa/schema_comparer.py` - `are_datatypes_equal_ignore_nullable` function
- **Test files**: `tests/test_schema_comparer.py`, `tests/test_structfield_comparer.py`
- **No API or behavior changes** - this is purely test coverage addition
