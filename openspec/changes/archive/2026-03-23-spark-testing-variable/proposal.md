## Why

Setting `SPARK_TESTING` environment variable before creating a SparkSession improves test performance by preventing Spark from spinning up UI and other unnecessary modules.

## What Changes

Refactor SparkSession creation from a static module-level import to a pytest fixture with maximal scope. All tests currently importing `spark` from `tests.spark` will instead receive it via fixture injection.

## Capabilities

### New Capabilities

- `spark-testing`: pytest fixture providing SparkSession with `SPARK_TESTING` env var set for improved test performance

### Modified Capabilities

- `tests/spark.py` (deleted): Static `spark` import replaced by `spark` fixture

## Impact

- All test files using `from tests.spark import spark` must be updated to use `spark` fixture parameter
- Type hints must follow project conventions (e.g., `spark: SparkSession`)
- No behavioral changes to tests; purely structural refactoring
- Old `tests/spark.py` must be deleted after migration
