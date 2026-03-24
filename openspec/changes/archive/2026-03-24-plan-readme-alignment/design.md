## Context

`README.md` is the canonical user-facing entrypoint and is also embedded in published docs (`docs/index.md`).
Current README content contains version-support statements and API coverage that diverge from the shipped package (`pyproject.toml`, exported API in `chispa/__init__.py`, and current CI matrix). This creates a documentation contract mismatch.

Constraints:
- Documentation-only change; no runtime behavior changes are allowed.
- Maintain backward compatibility in messaging; avoid wording that implies behavior changes.
- Document only public, currently shipped interfaces.
- Keep examples executable with current supported Python/PySpark targets.

Stakeholders:
- End users relying on README for install and usage decisions.
- Maintainers who use README as the source for docs site content.

## Goals / Non-Goals

**Goals:**
- Establish a single-source README contract aligned with current package metadata and test matrix.
- Remove outdated claims that can cause incorrect environment assumptions.
- Ensure README examples use stable public imports and options already implemented.
- Document missing but public options/helpers that users can rely on today.

**Non-Goals:**
- Introducing new comparison features, flags, or API exports.
- Refactoring internals or changing function semantics.
- Altering CI/test matrix, dependency constraints, or release policy.
- Producing performance benchmarks or benchmark methodology.

## Decisions

- Decision: Treat packaging metadata and CI matrix as source-of-truth for compatibility statements.
  - Rationale: These are maintained operational controls and reduce drift versus narrative-only docs.
  - Alternative considered: Keep broad historical compatibility language in README.
  - Why not alternative: Historical claims conflict with current install/runtime constraints and increase user setup failures.

- Decision: Document only top-level public API and clearly supported options.
  - Rationale: Top-level exports define user contract and minimize dependency on internal modules.
  - Alternative considered: Keep module-internal import examples for brevity.
  - Why not alternative: Internal import examples are brittle and can become stale without API guarantees.

- Decision: Scope README updates to corrections and additive clarifications only.
  - Rationale: Prevent accidental behavior redefinition in docs and preserve compatibility expectations.
  - Alternative considered: Rewrite README structure wholesale.
  - Why not alternative: Large rewrites increase risk of introducing new inconsistencies and review noise.

- Decision: Explicitly mark out-of-scope items in change artifacts.
  - Rationale: Prevent junior implementers from expanding into implementation or policy work.
  - Alternative considered: Implicit scope via changed lines only.
  - Why not alternative: Implicit scope invites hallucinated additions and off-target edits.

## Risks / Trade-offs

- [Risk] README language may still lag future dependency or CI changes. -> Mitigation: Anchor wording to currently tested ranges and avoid speculative future-state statements.
- [Risk] Adding more options/helpers to README may increase perceived complexity. -> Mitigation: Keep option descriptions concise and grouped under a dedicated section.
- [Risk] Public/private boundary confusion for borderline helpers. -> Mitigation: Include only symbols exported in `chispa/__init__.py` or explicitly user-facing functions already used in docs/tests.
- [Risk] Over-correction could remove useful context from historical notes. -> Mitigation: Preserve intent while replacing incorrect claims with present-tense, verifiable statements.

## Migration Plan

- No runtime migration required.
- Documentation consumers receive corrected guidance immediately after release of docs/package containing README update.
- Rollback is simple: revert README-only changes if wording issues are identified.

## Open Questions

- None for this artifact; scope and sources-of-truth are defined.
