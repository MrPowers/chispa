from chispa.prettytable import PrettyTable
from chispa.bcolors import *
import chispa.six as six


class SchemasNotEqualError(Exception):
   """The schemas are not equal"""
   pass


def assert_schema_equality(s1, s2, ignore_nullable=False, ignore_metadata=True):
    if not ignore_nullable and not ignore_metadata:
        assert_strict_schema_equality(s1, s2)
    else:
        assert_non_strict_schema_equality(s1, s2, ignore_nullable, ignore_metadata)


def assert_strict_schema_equality(s1, s2):
    if s1 != s2:
        t = PrettyTable(["schema1", "schema2"])
        zipped = list(six.moves.zip_longest(s1, s2))
        for sf1, sf2 in zipped:
            if sf1 == sf2:
                t.add_row([blue(sf1), blue(sf2)])
            else:
                t.add_row([sf1, sf2])
        raise SchemasNotEqualError("\n" + t.get_string())


def assert_non_strict_schema_equality(s1, s2, ignore_nullable=True, ignore_metadata=True):
    if not are_schemas_equal(s1, s2, ignore_nullable, ignore_metadata):
        t = PrettyTable(["schema1", "schema2"])
        zipped = list(six.moves.zip_longest(s1, s2))
        for sf1, sf2 in zipped:
            if are_structfields_equal(sf1, sf2, ignore_nullable, ignore_metadata):
                t.add_row([blue(sf1), blue(sf2)])
            else:
                t.add_row([sf1, sf2])
        raise SchemasNotEqualError("\n" + t.get_string())


def are_schemas_equal(s1, s2, ignore_nullable=True, ignore_metadata=True):
    if len(s1) != len(s2):
        return False
    zipped = list(six.moves.zip_longest(s1, s2))
    for sf1, sf2 in zipped:
        if not are_structfields_equal(sf1, sf2, ignore_nullable, ignore_metadata):
            return False
    return True


def are_structfields_equal(sf1, sf2, ignore_nullable=False, ignore_metadata=True):
    if not ignore_nullable and not ignore_metadata:
        return sf1 == sf2

    if sf1 is None and sf2 is not None:
        return False
    if sf1 is not None and sf2 is None:
        return False
    if sf1 is None and sf2 is None:
        return True

    # At this point, we know that both sf1 and sf2 are not None.
    if sf1.name != sf2.name:
        return False

    # Field names are equal, check the rest of the struct field.
    return (
        are_datatypes_equal(sf1.dataType, sf2.dataType, ignore_nullable, ignore_metadata) and
        (ignore_nullable or (sf1.nullable == sf2.nullable)) and
        (ignore_metadata or (sf1.metadata == sf2.metadata))
    )


def are_datatypes_equal(dt1, dt2, ignore_nullable=True, ignore_metadata=True):
    """Checks if datatypes are equal, descending into structs, arrays, and maps.
    """
    if dt1.typeName() != dt2.typeName():
        return False

    if dt1.typeName() == 'struct':
        return are_schemas_equal(dt1, dt2, ignore_nullable, ignore_metadata)
    # Account for array types by inspecting elementType.
    elif dt1.typeName() == 'array':
        return are_datatypes_equal(dt1.elementType, dt2.elementType, ignore_nullable, ignore_metadata)
    # Account for map types by inspecting keyType and valueType.
    elif dt1.typeName() == 'map':
        return (
            are_datatypes_equal(dt1.keyType, dt2.keyType, ignore_nullable, ignore_metadata) and
            are_datatypes_equal(dt1.valueType, dt2.valueType, ignore_nullable, ignore_metadata)
        )
    else:
        return True
