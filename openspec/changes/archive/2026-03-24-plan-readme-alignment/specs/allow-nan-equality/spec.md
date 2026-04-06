## ADDED Requirements

### Requirement: README documents allow_nan_equality behavior consistently
The project README SHALL describe `allow_nan_equality` behavior for DataFrame equality in a way that is consistent with the existing `allow-nan-equality` capability semantics.

#### Scenario: README describes opt-in NaN equality
- **WHEN** README explains NaN comparison behavior
- **THEN** it SHALL state that NaN equality is enabled only when `allow_nan_equality=True`

#### Scenario: README preserves strict default semantics
- **WHEN** README explains default DataFrame equality behavior
- **THEN** it SHALL indicate that NaN values are not treated as equal by default

#### Scenario: README example does not redefine behavior
- **WHEN** README contains code examples for NaN handling
- **THEN** the examples SHALL demonstrate existing behavior without introducing new semantics or flags
