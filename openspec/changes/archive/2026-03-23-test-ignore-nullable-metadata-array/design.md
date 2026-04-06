## Context

The bug investigation revealed insufficient test coverage for the `ignore_nullable=True` and `ignore_metadata=True` combination with array types in schema comparison.

## Goals / Non-Goals

**Goals:**
- Add test cases that verify `ignore_nullable=True` AND `ignore_metadata=True` work correctly together with array types

**Non-Goals:**
- No changes to production code (only test additions)
- No API or behavior changes

## Decisions

1. **Add tests to existing test files** rather than creating new test files
   - `tests/test_schema_comparer.py` - for schema-level tests
   - `tests/test_structfield_comparer.py` - for structfield-level tests

2. **Test scenarios to cover:**
   - Array of primitive types: different nullable on element + different metadata
   - Array of structs: different metadata on struct fields
   - Nested arrays: Array(Array(type)) with different metadata

## Risks / Trade-offs

- **Risk**: Tests may reveal actual bugs in the code
- **Mitigation**: If bugs are found, they should be documented and fixed as part of this change
