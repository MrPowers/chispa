## ADDED Requirements

### Requirement: Array types with ignore_nullable and ignore_metadata
When `ignore_nullable=True` and `ignore_metadata=True` are both set, the schema comparison SHALL correctly ignore nullable differences AND metadata differences in array element types.

#### Scenario: Array of primitives - different nullable on element
- **WHEN** comparing schemas with array element types that have different nullable flags
- **THEN** `are_datatypes_equal_ignore_nullable` SHALL return True when `ignore_metadata=True`

#### Scenario: Array of primitives - different metadata on element
- **WHEN** comparing schemas with array element types that have different metadata
- **THEN** `are_datatypes_equal_ignore_nullable` SHALL return True when `ignore_metadata=True`

#### Scenario: Array of structs - different metadata on struct fields
- **WHEN** comparing schemas with ArrayType(StructType) where struct fields have different metadata
- **THEN** `are_datatypes_equal_ignore_nullable` SHALL return True when `ignore_metadata=True`

#### Scenario: Nested arrays - different metadata
- **WHEN** comparing schemas with nested ArrayType(ArrayType) that have different metadata
- **THEN** `are_datatypes_equal_ignore_nullable` SHALL return True when `ignore_metadata=True`

#### Scenario: Array of structs - different nullable on struct field
- **WHEN** comparing schemas with ArrayType(StructType) where struct fields have different nullable
- **THEN** `are_datatypes_equal_ignore_nullable` SHALL return True when `ignore_metadata=True`
