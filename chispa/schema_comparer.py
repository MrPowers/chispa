from __future__ import annotations

import typing
from itertools import zip_longest

from prettytable import PrettyTable
from pyspark.sql.types import StructField, StructType

from chispa.bcolors import bcolors, line_blue, line_red
from chispa.formatting import blue


class SchemasNotEqualError(Exception):
    """The schemas are not equal"""

    pass


def print_schema_diff(
    s1: StructType, s2: StructType, ignore_nullable: bool, ignore_metadata: bool, output_format: str = "table"
) -> None:
    valid_output_formats = ["table", "tree"]
    if output_format == "table":
        schema_diff_table: PrettyTable = create_schema_comparison_table(s1, s2, ignore_nullable, ignore_metadata)
        print(schema_diff_table)
    elif output_format == "tree":
        schema_diff_tree: str = create_schema_comparison_tree(s1, s2, ignore_nullable, ignore_metadata)
        print(schema_diff_tree)
    else:
        raise ValueError(f"output_format must be one of {valid_output_formats}")


def create_schema_comparison_tree(s1: StructType, s2: StructType, ignore_nullable: bool, ignore_metadata: bool) -> str:
    def parse_schema_as_tree(s: StructType, indent: int) -> tuple[list[str], list[StructField]]:
        tree_lines = []
        fields = []

        for struct_field in s:
            nullable = "(nullable = true)" if struct_field.nullable else "(nullable = false)"
            struct_field_type = struct_field.dataType.typeName()

            struct_prefix = f"{indent * ' '}|{'-' * 2}"
            struct_as_string = f"{struct_field.name}: {struct_field_type} {nullable}"

            tree_lines += [f"{struct_prefix} {struct_as_string}"]

            if not struct_field_type == "struct":
                fields += [struct_field]
                continue

            tree_line_nested, fields_nested = parse_schema_as_tree(struct_field.dataType, indent + 4)  # type: ignore[arg-type]

            fields += [struct_field]
            tree_lines += tree_line_nested
            fields += fields_nested

        return tree_lines, fields

    tree_space = 6
    s1_tree, s1_fields = parse_schema_as_tree(s1, 0)
    s2_tree, s2_fields = parse_schema_as_tree(s2, 0)

    widest_line = max(len(line) for line in s1_tree)
    longest_tree = max(len(s1_tree), len(s2_tree))
    schema_gap = widest_line + tree_space

    tree = "\nschema1".ljust(schema_gap) + "schema2\n"
    for i in range(longest_tree):
        line1 = line2 = ""
        s1_field = s2_field = None

        if i < len(s1_tree):
            line1 = s1_tree[i]
            s1_field = s1_fields[i]
        if i < len(s2_tree):
            line2 = s2_tree[i]
            s2_field = s2_fields[i]

        tree_line = line1.ljust(schema_gap) + line2

        if are_structfields_equal(s1_field, s2_field, ignore_nullable, ignore_metadata):
            tree += line_blue(tree_line) + "\n"
        else:
            tree += line_red(tree_line) + "\n"

    tree += bcolors.NC
    return tree


def create_schema_comparison_table(
    s1: StructType, s2: StructType, ignore_nullable: bool, ignore_metadata: bool
) -> PrettyTable:
    t = PrettyTable(["schema1", "schema2"])
    zipped = list(zip_longest(s1, s2))
    for sf1, sf2 in zipped:
        if are_structfields_equal(sf1, sf2, ignore_nullable, ignore_metadata):
            t.add_row([blue(str(sf1)), blue(str(sf2))])
        else:
            t.add_row([sf1, sf2])
    return t


def check_if_schemas_are_wide(s1: StructType, s2: StructType) -> bool:
    contains_nested_structs = any(sf.dataType.typeName() == "struct" for sf in s1) or any(
        sf.dataType.typeName() == "struct" for sf in s2
    )
    contains_many_columns = len(s1) > 10 or len(s2) > 10
    return contains_nested_structs or contains_many_columns


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
            if not are_structfields_equal(sf1, sf2, ignore_nullable, ignore_metadata):
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
    if not are_schemas_equal_ignore_nullable(s1, s2):
        handle_schemas_not_equal(s1, s2, ignore_nullable=True, ignore_metadata=False)


# deprecate this.  ignore_nullable should be a flag.
def are_schemas_equal_ignore_nullable(s1: StructType, s2: StructType, ignore_metadata: bool = False) -> bool:
    if len(s1) != len(s2):
        return False
    zipped = list(zip_longest(s1, s2))
    for sf1, sf2 in zipped:
        if not are_structfields_equal(sf1, sf2, True, ignore_metadata):
            return False
    return True


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
        if not ignore_metadata and sf1.metadata != sf2.metadata:
            return False
        else:
            return are_datatypes_equal_ignore_nullable(sf1.dataType, sf2.dataType, ignore_metadata)  # type: ignore[no-any-return, no-untyped-call]


# deprecate this
@typing.no_type_check
def are_datatypes_equal_ignore_nullable(dt1, dt2, ignore_metadata: bool = False) -> bool:
    """Checks if datatypes are equal, descending into structs and arrays to
    ignore nullability.
    """
    if dt1.typeName() == dt2.typeName():
        # Account for array types by inspecting elementType.
        if dt1.typeName() == "array":
            return are_datatypes_equal_ignore_nullable(dt1.elementType, dt2.elementType, ignore_metadata)
        elif dt1.typeName() == "struct":
            return are_schemas_equal_ignore_nullable(dt1, dt2, ignore_metadata)
        else:
            # Some data types have additional attributes (e.g. precision and scale for Decimal),
            # and the type equality check must also check for equality of these attributes.
            return vars(dt1) == vars(dt2)
    else:
        return False
