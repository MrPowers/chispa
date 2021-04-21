from chispa.prettytable import PrettyTable
from chispa.bcolors import *
import chispa.six as six
from chispa.structfield_comparer import are_structfields_equal


class SchemasNotEqualError(Exception):
   """The schemas are not equal"""
   pass


def assert_schema_equality(s1, s2, ignore_nullable=False):
    if ignore_nullable:
        assert_schema_equality_ignore_nullable(s1, s2)
    else:
        assert_basic_schema_equality(s1, s2)


def assert_basic_schema_equality(s1, s2):
    if s1 != s2:
        t = PrettyTable(["schema1", "schema2"])
        zipped = list(six.moves.zip_longest(s1, s2))
        for sf1, sf2 in zipped:
            if sf1 == sf2:
                t.add_row([blue(sf1), blue(sf2)])
            else:
                t.add_row([sf1, sf2])
        raise SchemasNotEqualError("\n" + t.get_string())


def assert_schema_equality_ignore_nullable(s1, s2):
    if are_schemas_equal_ignore_nullable(s1, s2) == False:
        t = PrettyTable(["schema1", "schema2"])
        zipped = list(six.moves.zip_longest(s1, s2))
        for sf1, sf2 in zipped:
            if are_structfields_equal(sf1, sf2, True):
                t.add_row([blue(sf1), blue(sf2)])
            else:
                t.add_row([sf1, sf2])
        raise SchemasNotEqualError("\n" + t.get_string())


def are_schemas_equal_ignore_nullable(s1, s2):
    if len(s1) != len(s2):
        return False
    zipped = list(six.moves.zip_longest(s1, s2))
    for sf1, sf2 in zipped:
        names_equal = sf1.name == sf2.name
        types_equal = check_type_equal_ignore_nullable(sf1, sf2)
        if not names_equal or not types_equal:
          return False
    return True


def check_type_equal_ignore_nullable(sf1, sf2):
    """Checks StructField data types ignoring nullables.

    Handles array element types also.
    """
    dt1, dt2 = sf1.dataType, sf2.dataType
    if dt1.typeName() == dt2.typeName():
        # Account for array types by inspecting elementType.
        if dt1.typeName() == 'array':
            return dt1.elementType == dt2.elementType
        else:
            return True
    else:
        return False
