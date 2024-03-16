from chispa.prettytable import PrettyTable
from chispa.bcolors import *
import chispa.six as six


class SchemasNotEqualError(Exception):
    """The schemas are not equal"""

    pass


def create_schema_comparison_tree(s1, s2, ignore_nullable: bool) -> str:
    def create_schema_tree(s, indent: int, horizontal_char="-") -> list[str]:
        tree_string = []
        for struct_field in s:
            nullable = (
                "(nullable = true)" if struct_field.nullable else "(nullable = false)"
            )
            tree_string += [
                f"{indent * ' '}|{horizontal_char * 2} {struct_field.name}: {struct_field.dataType.typeName()} {nullable}"
            ]
            if struct_field.dataType.typeName() == "struct":
                tree_string += create_schema_tree(
                    struct_field.dataType, indent + 4, horizontal_char
                )
        return tree_string

    tree_space = 6
    horizontal_char = "-"

    s1_tree = create_schema_tree(s1, 0, horizontal_char)
    s2_tree = create_schema_tree(s2, 0, horizontal_char)

    widest_line = max(len(line) for line in s1_tree)
    tree_string_combined = "\n\nschema1".ljust(widest_line + tree_space) + "schema2\n"
    for i in range(max(len(s1_tree), len(s2_tree))):
        line1 = ""
        line2 = ""
        if i < len(s1_tree):
            line1 = s1_tree[i]
        if i < len(s2_tree):
            line2 = s2_tree[i]

        tree_string_line = line1.ljust(widest_line + tree_space) + line2

        if are_schema_strings_equal(line1, line2, ignore_nullable):
            tree_string_line = line_blue(tree_string_line)

        else:
            tree_string_line = line_red(tree_string_line)

        tree_string_combined += tree_string_line + "\n"

    tree_string_combined += bcolors.LightBlue
    return tree_string_combined


def are_schema_strings_equal(s1: str, s2: str, ignore_nullable: bool) -> bool:
    if not ignore_nullable:
        return s1 == s2

    s1_no_nullable = s1.replace("(nullable = true)", "").replace(
        "(nullable = false)", ""
    )
    s2_no_nullable = s2.replace("(nullable = true)", "").replace(
        "(nullable = false)", ""
    )
    return s1_no_nullable == s2_no_nullable


def create_schema_comparison_table(s1, s2):
    t = PrettyTable(["schema1", "schema2"])
    zipped = list(six.moves.zip_longest(s1, s2))
    for sf1, sf2 in zipped:
        if are_structfields_equal(sf1, sf2, True):
            t.add_row([blue(sf1), blue(sf2)])
        else:
            t.add_row([sf1, sf2])
    return t


def check_if_schemas_are_wide(s1, s2) -> bool:
    contains_nested_structs = any(
        sf.dataType.typeName() == "struct" for sf in s1
    ) or any(sf.dataType.typeName() == "struct" for sf in s2)
    contains_many_columns = len(s1) > 10 or len(s2) > 10
    return contains_nested_structs or contains_many_columns


def handle_schemas_not_equal(s1, s2, ignore_nullable: bool):
    schemas_are_wide = check_if_schemas_are_wide(s1, s2)
    if schemas_are_wide:
        error_message = create_schema_comparison_tree(s1, s2, ignore_nullable)
    else:
        t = create_schema_comparison_table(s1, s2)
        error_message = "\n" + t.get_string()
    raise SchemasNotEqualError(error_message)


def assert_schema_equality(s1, s2, ignore_nullable=False, ignore_metadata=False):
    if not ignore_nullable and not ignore_metadata:
        assert_basic_schema_equality(s1, s2)
    else:
        assert_schema_equality_full(s1, s2, ignore_nullable, ignore_metadata)


def assert_schema_equality_full(s1, s2, ignore_nullable=False, ignore_metadata=False):
    def inner(s1, s2, ignore_nullable, ignore_metadata):
        if len(s1) != len(s2):
            return False
        zipped = list(six.moves.zip_longest(s1, s2))
        for sf1, sf2 in zipped:
            if not are_structfields_equal(sf1, sf2, ignore_nullable, ignore_metadata):
                return False
        return True

    if not inner(s1, s2, ignore_nullable, ignore_metadata):
        handle_schemas_not_equal(s1, s2, ignore_nullable)


# deprecate this
# perhaps it is a little faster, but do we really need this?
# I think schema equality operations are really fast to begin with
def assert_basic_schema_equality(s1, s2):
    if s1 != s2:
        handle_schemas_not_equal(s1, s2, ignore_nullable=False)


# deprecate this.  ignore_nullable should be a flag.
def assert_schema_equality_ignore_nullable(s1, s2):
    if not are_schemas_equal_ignore_nullable(s1, s2):
        handle_schemas_not_equal(s1, s2, ignore_nullable=True)


# deprecate this.  ignore_nullable should be a flag.
def are_schemas_equal_ignore_nullable(s1, s2):
    if len(s1) != len(s2):
        return False
    zipped = list(six.moves.zip_longest(s1, s2))
    for sf1, sf2 in zipped:
        if not are_structfields_equal(sf1, sf2, True):
            return False
    return True


# "ignore_nullability" should be "ignore_nullable" for consistent terminology
def are_structfields_equal(sf1, sf2, ignore_nullability=False, ignore_metadata=False):
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
            return are_datatypes_equal_ignore_nullable(sf1.dataType, sf2.dataType)


# deprecate this
def are_datatypes_equal_ignore_nullable(dt1, dt2):
    """Checks if datatypes are equal, descending into structs and arrays to
    ignore nullability.
    """
    if dt1.typeName() == dt2.typeName():
        # Account for array types by inspecting elementType.
        if dt1.typeName() == "array":
            return are_datatypes_equal_ignore_nullable(dt1.elementType, dt2.elementType)
        elif dt1.typeName() == "struct":
            return are_schemas_equal_ignore_nullable(dt1, dt2)
        else:
            return True
    else:
        return False
