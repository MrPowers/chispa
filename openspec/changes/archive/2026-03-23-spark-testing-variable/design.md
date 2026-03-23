## Context

Currently `tests/spark.py` creates a SparkSession at module load time:
```python
spark = SparkSession.builder.master("local").appName("chispa").getOrCreate()
```

Tests import this via `from tests.spark import spark`. This prevents setting `SPARK_TESTING` before session creation since the import happens before any test configuration.

## Goals / Non-Goals

**Goals:**
- Set `SPARK_TESTING=1` before SparkSession creation for test performance
- Provide SparkSession via pytest fixture with session scope
- Migrate all existing tests to use fixture injection

**Non-Goals:**
- No changes to Spark version or configuration options
- No changes to test assertions or behavior
- No new test files; only refactor existing ones

## Decisions

### 1. Fixture placement in conftest.py

**Decision:** Add `spark` fixture to `tests/conftest.py`

**Rationale:** `conftest.py` is automatically loaded by pytest. Placing the fixture here ensures it's available to all tests without explicit imports. Session-scoped fixtures in conftest are also cleaned up properly at session end.

**Alternative considered:** Create a new `tests/fixtures.py` file.
- Rejected: Unnecessary file; conftest.py is the established location for shared fixtures in pytest projects.

### 2. Session scope

**Decision:** Use `@pytest.fixture(scope="session")`

**Rationale:** A single SparkSession should be reused across all tests for performance. Creating a new session per test would be expensive.

### 3. SPARK_TESTING setting

**Decision:** Set `os.environ["SPARK_TESTING"] = "1"` before creating the session inside the fixture.

**Rationale:** The environment variable must be set before `SparkSession.builder.getOrCreate()` is called to affect Spark's initialization behavior.

### 4. Type annotation

**Decision:** `spark: SparkSession` fixture parameter (following project conventions from AGENTS.md: "pyspark.sql.DataFrame, pyspark.sql.types.StructType, etc.")

**Rationale:** Project uses modern-style type hints with `from __future__ import annotations`.

### 5. Migration approach

**Decision:** Replace `from tests.spark import spark` with `spark: SparkSession` fixture parameter in each test file.

**Rationale:** This is a direct substitution; no other changes to test logic are needed since the fixture provides the same SparkSession object.

## Risks / Trade-offs

- **Risk:** Some test may rely on spark module being loaded for side effects.
  - **Mitigation:** The SparkSession is created eagerly when the module loads. Moving to fixture delays creation to first test use, but this is acceptable for tests.
  
- **Risk:** Tests that modify `spark` state (e.g., `spark.stop()`) could break other tests.
  - **Mitigation:** Document that tests must not call `spark.stop()`. Session-scoped fixture shares the same session across tests.

## Open Questions

None at this time.
