## Why

The README currently diverges from the shipped package behavior and support matrix, which can mislead users and create avoidable setup or usage errors. This change ensures the README is an accurate contract for current public behavior without altering runtime functionality.

## What Changes

- Update README compatibility statements to match current package constraints and tested matrix.
- Remove stale statements that describe deprecated or no-longer-true version support expectations.
- Align examples with current public API import style exposed by `chispa.__init__`.
- Add missing documentation for currently supported public options and helpers that are already implemented.
- Clarify boundaries: this change is documentation-only and does not modify code behavior.
- Explicitly define edge handling in docs scope:
- Include only options/helpers that are public and currently shipped.
- Exclude undocumented internals and deprecated internals unless needed for migration clarity.
- Keep existing user-facing semantics unchanged; do not redefine behavior in README.

## Capabilities

### New Capabilities

- `readme-alignment`: Establish a maintained README contract that reflects current supported versions, public API surface, and available comparison options.

### Modified Capabilities

- `allow-nan-equality`: Update related README references so documented behavior and usage examples remain consistent with existing spec-defined semantics.

## Impact

- Affected docs: `README.md` (source of published homepage/docs index).
- Affected generated docs surface: `docs/index.md` via README include.
- No runtime API changes, no dependency changes, no behavior changes, no migration required.
- Out of scope: implementation refactors, new comparison features, performance benchmarking work, CI matrix changes, and deprecation policy changes.
