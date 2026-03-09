# Proposal: Fix error if ignore_row_order=True with struct types

## Intent
When using ignore_row_order=True, I see the following error:

pyspark.errors.exceptions.captured.AnalysisException: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "my_col ASC NULLS FIRST" due to data type mismatch: The `sortorder` does not support ordering on type STRUCT<...>

## Scope
- ignore_row_order works well with struct types
- no existing code is broken, no regression or breaking chnages

## What Changes: Fix error if ignore_row_order=True with struct types
.

## Approach
If there is a struct-typed column (or any other column that does not support ordering), apply pyspark.functions.hash before passing to the "sort" call
