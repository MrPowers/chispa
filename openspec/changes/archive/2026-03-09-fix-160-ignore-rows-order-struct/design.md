## Context

When using `ignore_row_order=True` in chispa's `assert_df_equality`, DataFrames with struct-typed columns fail with:

```
pyspark.errors.exceptions.captured.AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE]
Cannot resolve "my_col ASC NULLS FIRST" due to data type mismatch:
The `sortorder` does not support ordering on type STRUCT<...>
```

This occurs because Spark's `sort()` function cannot order struct types directly. The current implementation attempts to sort all columns by name, which works for primitive types but fails for complex types like structs.

## Goals / Non-Goals

**Goals:**
- Enable `ignore_row_order=True` to work with struct-typed columns
- Maintain backward compatibility with existing code using primitive types
- Apply minimal, targeted fix using Spark's `hash()` function

**Non-Goals:**
- No changes to the core equality comparison logic
- No performance optimization beyond what's needed to fix the error
- No new public API surface

## Decisions

### Decision: Use `hash()` for struct columns before sorting

**Rationale:** Spark's `hash()` function converts any column type (including structs) into a comparable integer hash. By applying `hash()` only to struct columns before the `sort()` call, we can order rows without changing the semantic meaning of the comparison.

**Alternatives considered:**
1. **Cast struct to string**: Would work but loses type safety and is slower
2. **Flatten struct columns**: Adds complexity and changes column structure temporarily
3. **Skip sorting for struct columns**: Would break the ignore_row_order guarantee

The `hash()` approach is minimal, preserves the existing sort-based algorithm, and has no impact on primitive type columns.

## Risks / Trade-offs

**[Hash collision risk]** → Mitigation: Spark's `hash()` uses MurmurHash3 with 32-bit output. Collision probability is extremely low for typical DataFrame sizes. The hash is only used for row ordering, not equality comparison—final equality still compares actual values.

**[Performance overhead]** → Mitigation: `hash()` is applied only to struct columns. For DataFrames without structs, there is zero overhead. For struct columns, the hash computation is negligible compared to the sort operation.

**[Non-deterministic ordering]** → Mitigation: Hash values are deterministic for identical inputs. Rows with identical struct values will hash to the same value, maintaining consistent ordering.

## Open Questions

None—this is a straightforward fix with a clear implementation path.
