## Context

`assert_df_equality` currently supports `allow_nan_equality=True` for top-level scalar float fields, but not for nested values inside arrays. In Python equality semantics, `float("nan") != float("nan")`, so list equality fails even when both arrays contain `NaN` at the same positions.

Constraints:
- Top priority is no regression and no breaking changes.
- Public API shape and defaults must stay unchanged.
- Existing behavior for `allow_nan_equality=False` must remain identical.
- Scope is limited to `assert_df_equality` behavior covered in the proposal.

## Goals / Non-Goals

**Goals:**
- Make `allow_nan_equality=True` correctly treat array fields with aligned `NaN` values as equal.
- Keep mismatch detection strict for array length, index, and non-`NaN` value differences.
- Preserve all existing behavior outside the targeted scenario.
- Add regression tests that lock this behavior and protect against future regressions.

**Non-Goals:**
- No changes to function signatures, flags, or defaults.
- No change to behavior of `allow_nan_equality=False`.
- No expansion to `assert_approx_df_equality` in this change.
- No refactor of unrelated comparison paths.

## Decisions

1. Introduce deep NaN-aware value comparison for row fields when `allow_nan_equality=True`.
   - Rationale: current scalar-only helper does not handle nested containers.
   - Alternative considered: normalize arrays before comparison in DataFrame layer; rejected because it is more invasive and risks side effects.

2. Apply deep comparison only in the `allow_nan_equality=True` path.
   - Rationale: this minimizes blast radius and preserves established default semantics.
   - Alternative considered: replace all row equality logic with deep comparison; rejected due to higher regression risk.

3. Validate behavior with focused regression tests at DataFrame and row-comparer levels.
   - Rationale: direct tests on the failing scenario plus targeted unit coverage reduce risk and ensure stable behavior.
   - Alternative considered: only add high-level DataFrame tests; rejected because low-level helper behavior would be under-specified.

4. Keep exact index-based equality rules for arrays.
   - Rationale: existing equality semantics are positional; allowing non-positional matching would be a behavior change.
   - Alternative considered: set-like matching for arrays; rejected as breaking and ambiguous.

## Risks / Trade-offs

- [Risk] Recursive comparison can accidentally broaden semantics beyond arrays. -> Mitigation: limit comparisons to existing value structures and preserve strict type/shape checks.
- [Risk] Error rendering still highlights mismatched array cells using old per-field display logic. -> Mitigation: keep display behavior unchanged in this change and validate pass/fail correctness first.
- [Risk] Nested-edge handling (e.g., mixed containers) may introduce subtle regressions. -> Mitigation: add explicit tests for representative nested cases and keep implementation minimal.

## Migration Plan

- No migration required for users.
- Deploy as a backward-compatible bug fix in normal release flow.
- Rollback strategy: revert this change set if unexpected comparison regressions are reported.

## Open Questions

- None for this scoped change.
