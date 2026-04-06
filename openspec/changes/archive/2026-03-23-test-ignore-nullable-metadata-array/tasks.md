## 1. Add tests to test_schema_comparer.py

- [x] 1.1 Add test: Array of primitives - different nullable on element with ignore_nullable + ignore_metadata
- [x] 1.2 Add test: Array of primitives - different metadata on element with ignore_nullable + ignore_metadata
- [x] 1.3 Add test: Array of structs - different metadata on struct fields with ignore_nullable + ignore_metadata
- [x] 1.4 Add test: Nested arrays - different metadata with ignore_nullable + ignore_metadata
- [x] 1.5 Add test: Array of structs - different nullable on struct field with ignore_nullable + ignore_metadata

## 2. Add tests to test_structfield_comparer.py

- [x] 2.1 Add test: are_structfields_equal with array of structs - different metadata but ignore_metadata=True

## 3. Verify

- [x] 3.1 Run all tests to verify they pass
- [x] 3.2 Run linting and type checking (make check)
