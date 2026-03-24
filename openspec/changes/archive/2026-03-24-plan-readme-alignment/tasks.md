## 1. Source-of-truth validation

- [x] 1.1 Extract current Python support constraints from `pyproject.toml` and confirm wording target for README compatibility section.
- [x] 1.2 Extract currently tested PySpark versions from CI workflow matrix and map to a stable README compatibility statement.
- [x] 1.3 Verify public API exports in `chispa/__init__.py` to determine which imports/options/helpers are eligible for README documentation.

## 2. README alignment updates

- [x] 2.1 Replace outdated compatibility text in `README.md` with statements consistent with package constraints and tested CI matrix.
- [x] 2.2 Remove stale speculative/version-roadmap and unresolved TODO benchmark language from `README.md`.
- [x] 2.3 Update DataFrame equality import examples to use top-level public `chispa` imports instead of wildcard internal-module imports.
- [x] 2.4 Add concise README documentation for shipped public DataFrame comparison options and helper APIs currently missing from the docs.
- [x] 2.5 Ensure `allow_nan_equality` README wording and examples remain consistent with existing strict-default and opt-in semantics.

## 3. Verification and acceptance

- [x] 3.1 Cross-check updated README content against spec requirements in `specs/readme-alignment/spec.md` and `specs/allow-nan-equality/spec.md`.
- [x] 3.2 Run README example validation tests (`tests/test_readme_examples.py`) and address any documentation-induced test failures.
- [x] 3.3 Run project quality checks (`make check`) to confirm documentation edits did not introduce repository-level regressions.
