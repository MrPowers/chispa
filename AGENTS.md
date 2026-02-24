# Agent Guide for chispa

This document provides essential information for AI agents working on the `chispa` codebase.

## 🛠 Development Commands

The project uses `poetry` for dependency management and a `Makefile` for common tasks.

### Environment Setup

```bash
make install # Installs dependencies using poetry
```

Alternative way:

```bash
poetry install --all-groups --all-extras
```

### Testing
- **Run all tests:** `make test` (runs pytest with coverage) / `poetry run pytest -vvv` (`-v` / `-vv` / etc.)
- **Run a single test file:** `poetry run pytest tests/test_dataframe_comparer.py`
- **Run a specific test:** `poetry run pytest tests/test_dataframe_comparer.py::describe_assert_df_equality::it_throws_with_schema_mismatches`
- **Run tests without coverage:** `poetry run pytest tests`
- **Coverage Report (HTML):** `make test-cov-html`
- **Coverage Report (XML):** `make test-cov-xml`

### Linting & Type Checking
- **Full Check:** `make check` (runs pre-commit hooks and mypy) (`pre-commit run --all-files`)
- **Pre-commit hooks:** `poetry run pre-commit run -a`
- **Ruff (Lint & Format):** 
  - `poetry run ruff check .`
  - `poetry run ruff format .`
- **Mypy (Type Check):** `poetry run mypy chispa`
- **Mypy Strictness:** Configured in `pyproject.toml` with `strict = true`.

### Documentation
- **Build & Serve:** `make docs` (uses mkdocs)
- **Build test:** `make docs-test` (checks if docs build without warnings)

## 🎨 Code Style & Conventions

### General
- **Python Version:** Target Python 3.10 and above.
- **Line Length:** 120 characters (configured in `pyproject.toml`).
- **Encoding:** Always use UTF-8.

### Imports
- **Required Header:** Every file MUST start with `from __future__ import annotations`.
- **Order:**
  1. `from __future__ import annotations`
  2. Standard library imports
  3. Third-party library imports (e.g., `pyspark`, `pytest`)
  4. Local project imports
- **Formatting:** Alphabetical within groups. Prefer absolute imports.

### Typing
- **Strict Typing:** The project uses `mypy` in strict mode. All functions should have type hints for arguments and return values.
- **Type Aliases:** Use `from collections.abc import Callable` instead of `typing.Callable`.
- **Spark Types:** Use `pyspark.sql.DataFrame`, `pyspark.sql.types.StructType`, etc.
- **Modern Typing:** Use modern-style typing and prefer to add `from __future__ import annotations` over `from typing import List`

### Naming Conventions
- **Functions & Variables:** `snake_case` (e.g., `assert_df_equality`, `ignore_nullable`).
- **Classes:** `PascalCase` (e.g., `DataFramesNotEqualError`, `FormattingConfig`).
- **Constants:** `UPPER_SNAKE_CASE`.
- **Test Functions:** 
  - Use `pytest-describe` pattern.
  - Wrapper: `describe_<feature_name>()`
  - Test case: `it_<behavior_description>()`

### Error Handling
- **Custom Exceptions:** Define specific exception classes for library-specific errors (e.g., `DataFramesNotEqualError`, `SchemasNotEqualError`).
- **Informative Messages:** Exceptions should provide clear feedback on what mismatched (e.g., column names, data types, or specific row content).

### Formatting
- The project uses `ruff` for formatting. Adhere to its rules.
- Prefer double quotes for strings unless the string contains double quotes.

## 🧪 Testing Guidelines

- **Framework:** `pytest` with `pytest-describe`.
- **Spark Session:** Use the shared Spark session from `tests.spark`.
  ```python
  from .spark import spark
  ```
- **Assertions:** 
  - Use `pytest.raises` to verify that expected exceptions are thrown.
  - Test both positive cases (equality) and negative cases (mismatch).
- **Data for Tests:** Small, representative DataFrames created using `spark.createDataFrame()`.

## 📁 Project Structure

- `chispa/`: Main package containing comparison logic.
  - `dataframe_comparer.py`: Core functions for DataFrame comparison.
  - `column_comparer.py`: Column-level comparisons.
  - `schema_comparer.py`: Schema-level comparisons.
  - `formatting/`: Logic for terminal output formatting and coloring.
- `tests/`: Test suite mirroring the package structure.
- `docs/`: Documentation sources (mkdocs).

## 📝 Documentation
- Use docstrings for public functions and classes.
- Follow the existing style: concise one-liners for simple functions, more detailed for complex ones.
- Update `README.md` if adding new public-facing features.

## 🤖 AI Agent Specifics

**!!!IMPORTANT!!!**
This project has a lot of users! And most of them are not pinning the version in requirements! The top priority for any agent or LLM working with this codebase is to avoid **breaking changes** as much as possible until explicitly requested! No cowboy-style refactoring of anything just because "it looks bad"! Instead follow the strict rule: **DON'T TOUCH IF IT'S WORKING** !!

Any possible breaking change should be explicitly highlighted to the human-in-a-loop and require an **explicit** approve!
Codebase should stay AS IS as long as it possible!

Avoid the case when adding new functionality anyhow chnage the existing behaviour! This project is OLD and a lot of people are relying on it's stability!
**!!!!!!END!!!!!!**

- **Back-compatibility:** Think about the project like it is behind the space-program and should be back-compatible as much as possible.
- **Proactiveness:** Always run `make check` before considering a task complete.
- **Safety:** Do not modify `pyproject.toml` or `Makefile` unless explicitly requested.
- **Context:** Always check `chispa/__init__.py` to see what is exposed as the public API.
- **No Side Effects:** Comparison functions should not modify the input DataFrames. Use `transforms` or local copies if modification is needed for comparison.
