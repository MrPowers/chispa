from __future__ import annotations

import typing
import warnings
from itertools import zip_longest

from prettytable import PrettyTable
from pyspark.sql.types import StructField, StructType

from chispa.bcolors import bcolors, line_blue, line_red
from chispa.common_enums import OutputFormat, TypeName
from chispa.formatting import blue


class SchemasNotEqualError(Exception):
    """The schemas are not equal"""

    pass


def print_schema_diff(
    s1: StructType,
    s2: StructType,
    ignore_nullable: bool,
    ignore_metadata: bool,
    output_format: OutputFormat = OutputFormat.TABLE,
) -> None:
    if output_format == OutputFormat.TABLE:
        schema_diff_table: PrettyTable = create_schema_comparison_table(s1, s2, ignore_nullable, ignore_metadata)
        print(schema_diff_table)
    elif output_format == OutputFormat.TREE:
        schema_diff_tree: str = create_schema_comparison_tree(s1, s2, ignore_nullable, ignore_metadata)
        print(schema_diff_tree)
    else:
        raise ValueError(f"output_format must be one of {OutputFormat.__members__}")


def create_schema_comparison_tree(s1: StructType, s2: StructType, ignore_nullable: bool, ignore_metadata: bool) -> str:
    """Build a side-by-side tree diff of two schemas, expanding struct/array/map types."""
    lines: list[tuple[str, str, bool]] = []  # (left_text, right_text, is_equal)
    _compare_struct_fields(s1, s2, ignore_nullable, ignore_metadata, indent=0, lines=lines)

    if not lines:
        return "\nschema1    schema2\n" + bcolors.NC

    tree_space = 6
    widest_left = max(len(left) for left, _, _ in lines)
    schema_gap = widest_left + tree_space

    tree = "\nschema1".ljust(schema_gap) + "schema2\n"
    for left, right, is_equal in lines:
        tree_line = left.ljust(schema_gap) + right
        if is_equal:
            tree += line_blue(tree_line) + "\n"
        else:
            tree += line_red(tree_line) + "\n"
    tree += bcolors.NC
    return tree


def _format_nullable(nullable: bool) -> str:
    return "(nullable = true)" if nullable else "(nullable = false)"


def _format_contains_null(contains_null: bool) -> str:
    return "(containsNull = true)" if contains_null else "(containsNull = false)"


def _format_value_contains_null(value_contains_null: bool) -> str:
    return "(valueContainsNull = true)" if value_contains_null else "(valueContainsNull = false)"


def _prefix(indent: int) -> str:
    return f"{indent * ' '}|--"


def _format_type(dt: typing.Any) -> str:
    """Format a datatype for display: use typeName() for complex types, simpleString() for leaves."""
    tn: str = dt.typeName()
    if tn in (TypeName.STRUCT, TypeName.ARRAY, TypeName.MAP):
        return tn
    return str(dt.simpleString())


def _are_leaf_types_equal(dt1: typing.Any, dt2: typing.Any) -> bool:
    """Compare two non-complex datatypes including parameterized attributes (e.g. precision/scale)."""
    if dt1.typeName() != dt2.typeName():
        return False
    return bool(vars(dt1) == vars(dt2))


def _are_element_types_shallow_equal(dt1: typing.Any, dt2: typing.Any) -> bool:
    """Shallow-compare two datatypes: full attribute check for leaves, typeName only for complex types."""
    tn1: str = dt1.typeName()
    if tn1 != dt2.typeName():
        return False
    if tn1 in (TypeName.STRUCT, TypeName.ARRAY, TypeName.MAP):
        return True  # children compared on separate lines
    return bool(vars(dt1) == vars(dt2))


def _compare_struct_fields(
    s1: StructType,
    s2: StructType,
    ignore_nullable: bool,
    ignore_metadata: bool,
    indent: int,
    lines: list[tuple[str, str, bool]],
) -> None:
    """Compare struct fields pairwise and append tree lines."""
    pfx = _prefix(indent)
    for sf1, sf2 in zip_longest(s1, s2):
        # Format the field line for each side
        left = f"{pfx} {sf1.name}: {_format_type(sf1.dataType)} {_format_nullable(sf1.nullable)}" if sf1 else ""
        right = f"{pfx} {sf2.name}: {_format_type(sf2.dataType)} {_format_nullable(sf2.nullable)}" if sf2 else ""

        # Shallow equality: name, type name, nullable, metadata (not children)
        is_equal = _are_fields_shallow_equal(sf1, sf2, ignore_nullable, ignore_metadata)
        lines.append((left, right, is_equal))

        # Recurse into complex child types
        dt1 = sf1.dataType if sf1 else None
        dt2 = sf2.dataType if sf2 else None
        _compare_datatypes(dt1, dt2, ignore_nullable, ignore_metadata, indent + 4, lines)


def _are_fields_shallow_equal(
    sf1: StructField | None,
    sf2: StructField | None,
    ignore_nullable: bool,
    ignore_metadata: bool,
) -> bool:
    """Compare only the field's own properties (name, type name, nullable, metadata), not children."""
    if sf1 is None and sf2 is None:
        return True
    if sf1 is None or sf2 is None:
        return False
    if sf1.name != sf2.name:
        return False
    if not ignore_nullable and sf1.nullable != sf2.nullable:
        return False
    if not ignore_metadata and sf1.metadata != sf2.metadata:
        return False
    # For non-complex types, compare full type attributes (e.g. DecimalType precision/scale)
    type_name = sf1.dataType.typeName()
    if type_name not in (TypeName.STRUCT, TypeName.ARRAY, TypeName.MAP):
        return _are_leaf_types_equal(sf1.dataType, sf2.dataType)
    return sf1.dataType.typeName() == sf2.dataType.typeName()


def _compare_datatypes(
    dt1: typing.Any,
    dt2: typing.Any,
    ignore_nullable: bool,
    ignore_metadata: bool,
    indent: int,
    lines: list[tuple[str, str, bool]],
) -> None:
    """Recursively expand and compare complex datatypes (struct, array, map)."""
    tn1 = dt1.typeName() if dt1 else None
    tn2 = dt2.typeName() if dt2 else None

    # Both are structs (or one is struct, other is missing/different type)
    if tn1 == TypeName.STRUCT or tn2 == TypeName.STRUCT:
        s1 = dt1 if tn1 == TypeName.STRUCT else StructType([])
        s2 = dt2 if tn2 == TypeName.STRUCT else StructType([])
        _compare_struct_fields(s1, s2, ignore_nullable, ignore_metadata, indent, lines)

    elif tn1 == TypeName.ARRAY or tn2 == TypeName.ARRAY:
        _compare_array_types(
            dt1 if tn1 == TypeName.ARRAY else None,
            dt2 if tn2 == TypeName.ARRAY else None,
            ignore_nullable,
            ignore_metadata,
            indent,
            lines,
        )

    elif tn1 == TypeName.MAP or tn2 == TypeName.MAP:
        _compare_map_types(
            dt1 if tn1 == TypeName.MAP else None,
            dt2 if tn2 == TypeName.MAP else None,
            ignore_nullable,
            ignore_metadata,
            indent,
            lines,
        )


def _compare_array_types(
    dt1: typing.Any,
    dt2: typing.Any,
    ignore_nullable: bool,
    ignore_metadata: bool,
    indent: int,
    lines: list[tuple[str, str, bool]],
) -> None:
    """Expand array types showing 'element: <type> (containsNull = ...)'."""
    pfx = _prefix(indent)

    left = f"{pfx} element: {_format_type(dt1.elementType)} {_format_contains_null(dt1.containsNull)}" if dt1 else ""
    right = f"{pfx} element: {_format_type(dt2.elementType)} {_format_contains_null(dt2.containsNull)}" if dt2 else ""

    # Element line equality: element type and containsNull
    is_equal = True
    if dt1 is None or dt2 is None:
        is_equal = dt1 is None and dt2 is None
    else:
        if not ignore_nullable and dt1.containsNull != dt2.containsNull:
            is_equal = False
        elif not _are_element_types_shallow_equal(dt1.elementType, dt2.elementType):
            is_equal = False
    lines.append((left, right, is_equal))

    # Recurse into element type if complex
    elem1 = dt1.elementType if dt1 else None
    elem2 = dt2.elementType if dt2 else None
    _compare_datatypes(elem1, elem2, ignore_nullable, ignore_metadata, indent + 4, lines)


def _compare_map_types(
    dt1: typing.Any,
    dt2: typing.Any,
    ignore_nullable: bool,
    ignore_metadata: bool,
    indent: int,
    lines: list[tuple[str, str, bool]],
) -> None:
    """Expand map types showing key and value sub-lines."""
    pfx = _prefix(indent)

    # Key line
    left_key = f"{pfx} key: {_format_type(dt1.keyType)}" if dt1 else ""
    right_key = f"{pfx} key: {_format_type(dt2.keyType)}" if dt2 else ""
    key_equal = True
    if dt1 is None or dt2 is None:
        key_equal = dt1 is None and dt2 is None
    elif not _are_element_types_shallow_equal(dt1.keyType, dt2.keyType):
        key_equal = False
    lines.append((left_key, right_key, key_equal))

    # Recurse into key type if complex
    k1 = dt1.keyType if dt1 else None
    k2 = dt2.keyType if dt2 else None
    _compare_datatypes(k1, k2, ignore_nullable, ignore_metadata, indent + 4, lines)

    # Value line
    left_val = (
        f"{pfx} value: {_format_type(dt1.valueType)} {_format_value_contains_null(dt1.valueContainsNull)}"
        if dt1
        else ""
    )
    right_val = (
        f"{pfx} value: {_format_type(dt2.valueType)} {_format_value_contains_null(dt2.valueContainsNull)}"
        if dt2
        else ""
    )
    val_equal = True
    if dt1 is None or dt2 is None:
        val_equal = dt1 is None and dt2 is None
    else:
        if not ignore_nullable and dt1.valueContainsNull != dt2.valueContainsNull:
            val_equal = False
        elif not _are_element_types_shallow_equal(dt1.valueType, dt2.valueType):
            val_equal = False
    lines.append((left_val, right_val, val_equal))

    # Recurse into value type if complex
    v1 = dt1.valueType if dt1 else None
    v2 = dt2.valueType if dt2 else None
    _compare_datatypes(v1, v2, ignore_nullable, ignore_metadata, indent + 4, lines)


def create_schema_comparison_table(
    s1: StructType, s2: StructType, ignore_nullable: bool, ignore_metadata: bool
) -> PrettyTable:
    t = PrettyTable(["schema1", "schema2"])
    zipped = list(zip_longest(s1, s2))
    for sf1, sf2 in zipped:
        if are_structfields_equal(sf1, sf2, ignore_nullability=ignore_nullable, ignore_metadata=ignore_metadata):
            t.add_row([blue(str(sf1)), blue(str(sf2))])
        else:
            t.add_row([sf1, sf2])
    return t


def _has_complex_type(sf: StructField) -> bool:
    return sf.dataType.typeName() in (TypeName.STRUCT, TypeName.ARRAY, TypeName.MAP)


def check_if_schemas_are_wide(s1: StructType, s2: StructType) -> bool:
    contains_complex_types = any(_has_complex_type(sf) for sf in s1) or any(_has_complex_type(sf) for sf in s2)
    contains_many_columns = len(s1) > 10 or len(s2) > 10
    return contains_complex_types or contains_many_columns


def handle_schemas_not_equal(s1: StructType, s2: StructType, ignore_nullable: bool, ignore_metadata: bool) -> None:
    schemas_are_wide = check_if_schemas_are_wide(s1, s2)
    if schemas_are_wide:
        error_message = create_schema_comparison_tree(s1, s2, ignore_nullable, ignore_metadata)
    else:
        t = create_schema_comparison_table(s1, s2, ignore_nullable, ignore_metadata)
        error_message = "\n" + t.get_string()
    raise SchemasNotEqualError(error_message)


def assert_schema_equality(
    s1: StructType, s2: StructType, ignore_nullable: bool = False, ignore_metadata: bool = False
) -> None:
    if not ignore_nullable and not ignore_metadata:
        assert_basic_schema_equality(s1, s2)
    else:
        assert_schema_equality_full(s1, s2, ignore_nullable, ignore_metadata)


def assert_schema_equality_full(
    s1: StructType, s2: StructType, ignore_nullable: bool = False, ignore_metadata: bool = False
) -> None:
    def inner(s1: StructType, s2: StructType, ignore_nullable: bool, ignore_metadata: bool) -> bool:
        if len(s1) != len(s2):
            return False
        zipped = list(zip_longest(s1, s2))
        for sf1, sf2 in zipped:
            if not are_structfields_equal(
                sf1, sf2, ignore_nullability=ignore_nullable, ignore_metadata=ignore_metadata
            ):
                return False
        return True

    if not inner(s1, s2, ignore_nullable, ignore_metadata):
        handle_schemas_not_equal(s1, s2, ignore_nullable, ignore_metadata)


# deprecate this
# perhaps it is a little faster, but do we really need this?
# I think schema equality operations are really fast to begin with
def assert_basic_schema_equality(s1: StructType, s2: StructType) -> None:
    if s1 != s2:
        handle_schemas_not_equal(s1, s2, ignore_nullable=False, ignore_metadata=False)


# deprecate this.  ignore_nullable should be a flag.
def assert_schema_equality_ignore_nullable(s1: StructType, s2: StructType) -> None:
    if not are_schemas_equal(s1, s2, ignore_nullable=True):
        handle_schemas_not_equal(s1, s2, ignore_nullable=True, ignore_metadata=False)


def are_schemas_equal(
    s1: StructType, s2: StructType, ignore_nullable: bool = False, ignore_metadata: bool = False
) -> bool:
    if len(s1) != len(s2):
        return False
    zipped = list(zip_longest(s1, s2))
    for sf1, sf2 in zipped:
        if not are_structfields_equal(sf1, sf2, ignore_nullability=ignore_nullable, ignore_metadata=ignore_metadata):
            return False
    return True


def are_schemas_equal_ignore_nullable(s1: StructType, s2: StructType, ignore_metadata: bool = False) -> bool:
    """Deprecated: use are_schemas_equal instead."""
    warnings.warn(
        "are_schemas_equal_ignore_nullable is deprecated. Use `are_schemas_equal(s1, s2, ignore_nullable=True)` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return are_schemas_equal(s1, s2, ignore_nullable=True, ignore_metadata=ignore_metadata)


# "ignore_nullability" should be "ignore_nullable" for consistent terminology
def are_structfields_equal(
    sf1: StructField | None, sf2: StructField | None, ignore_nullability: bool = False, ignore_metadata: bool = False
) -> bool:
    if not ignore_nullability and not ignore_metadata:
        return sf1 == sf2
    else:
        if sf1 is None or sf2 is None:
            if sf1 is None and sf2 is None:
                return True
            else:
                return False
        if sf1.name != sf2.name:
            return False
        if not ignore_nullability and sf1.nullable != sf2.nullable:
            return False
        if not ignore_metadata and sf1.metadata != sf2.metadata:
            return False
        return are_datatypes_equal(
            sf1.dataType,
            sf2.dataType,
            ignore_nullable=ignore_nullability,
            ignore_metadata=ignore_metadata,
        )


def are_datatypes_equal(
    dt1: typing.Any, dt2: typing.Any, ignore_nullable: bool = False, ignore_metadata: bool = False
) -> bool:
    """Checks if datatypes are equal, descending into structs, arrays and maps,
    optionally ignoring nullability and/or metadata.
    """
    if dt1.typeName() != dt2.typeName():
        return False
    if dt1.typeName() == TypeName.ARRAY:
        if not ignore_nullable and dt1.containsNull != dt2.containsNull:
            return False
        return are_datatypes_equal(dt1.elementType, dt2.elementType, ignore_nullable, ignore_metadata)
    elif dt1.typeName() == TypeName.STRUCT:
        return are_schemas_equal(dt1, dt2, ignore_nullable, ignore_metadata)
    elif dt1.typeName() == TypeName.MAP:
        if not ignore_nullable and dt1.valueContainsNull != dt2.valueContainsNull:
            return False
        return are_datatypes_equal(dt1.keyType, dt2.keyType, ignore_nullable, ignore_metadata) and are_datatypes_equal(
            dt1.valueType, dt2.valueType, ignore_nullable, ignore_metadata
        )
    else:
        return bool(vars(dt1) == vars(dt2))


def are_datatypes_equal_ignore_nullable(dt1: typing.Any, dt2: typing.Any, ignore_metadata: bool = False) -> bool:
    """Deprecated: use are_datatypes_equal instead."""
    warnings.warn(
        "are_datatypes_equal_ignore_nullable is deprecated. Use `are_datatypes_equal(dt1, dt2, ignore_nullable=True)` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return are_datatypes_equal(dt1, dt2, ignore_nullable=True, ignore_metadata=ignore_metadata)
