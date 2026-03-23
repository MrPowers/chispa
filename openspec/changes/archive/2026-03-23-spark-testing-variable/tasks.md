## 1. Add spark fixture to conftest.py

- [x] 1.1 Add `import os` to tests/conftest.py
- [x] 1.2 Add `import pytest` to tests/conftest.py (if not present)
- [x] 1.3 Add `from pyspark.sql import SparkSession` to tests/conftest.py
- [x] 1.4 Create `spark` fixture with `@pytest.fixture(scope="session")` that sets `SPARK_TESTING` env var before creating SparkSession

## 2. Migrate test files to use fixture

- [x] 2.1 Update `tests/test_dataframe_comparer.py` to use `spark: SparkSession` fixture parameter instead of `from .spark import spark`
- [x] 2.2 Update `tests/test_deprecated.py` to use `spark: SparkSession` fixture parameter instead of `from .spark import spark`
- [x] 2.3 Update `tests/test_column_comparer.py` to use `spark: SparkSession` fixture parameter instead of `from .spark import spark`
- [x] 2.4 Update `tests/test_rows_comparer.py` to use `spark: SparkSession` fixture parameter instead of `from .spark import spark`

## 3. Delete old spark module

- [x] 3.1 Delete `tests/spark.py`

## 4. Verify

- [x] 4.1 Run `make test` to ensure all tests pass
- [x] 4.2 Run `make check` to ensure linting and type checking pass
