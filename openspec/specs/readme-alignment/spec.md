## Purpose
Define README content requirements so published documentation reflects current, shipped behavior and supported compatibility guarantees.

## Requirements

### Requirement: README compatibility statements reflect current support contract
The project README SHALL state compatibility in a way that is consistent with currently shipped package constraints and actively tested runtime versions.

#### Scenario: Python compatibility statement matches package constraint
- **WHEN** README declares supported Python versions
- **THEN** the statement SHALL be compatible with the package metadata Python constraint

#### Scenario: PySpark compatibility statement matches tested support
- **WHEN** README declares supported PySpark versions
- **THEN** the statement SHALL be compatible with the currently tested PySpark release lines in CI

### Requirement: README examples use stable public API imports
README usage examples SHALL use public imports exposed by the package root when such exports exist.

#### Scenario: DataFrame equality example import
- **WHEN** README includes a DataFrame equality usage example
- **THEN** the example SHALL import from the top-level `chispa` public API instead of wildcard imports from internal modules

### Requirement: README documents shipped public comparison options
README SHALL document currently shipped, public DataFrame comparison options and helper APIs that users can rely on.

#### Scenario: Public option coverage is explicit
- **WHEN** README documents DataFrame equality options
- **THEN** it SHALL include currently supported public flags used to control schema/content comparison behavior

#### Scenario: Public helper coverage is explicit
- **WHEN** README references helper utilities
- **THEN** it SHALL include shipped public helpers used for common equality checks

### Requirement: README excludes stale or speculative product statements
README SHALL not contain unresolved TODO placeholders or speculative future-version policy statements presented as current guidance.

#### Scenario: Outdated roadmap language removal
- **WHEN** README content includes historical or speculative compatibility roadmap statements
- **THEN** the published README SHALL remove or replace them with verifiable present-tense guidance
