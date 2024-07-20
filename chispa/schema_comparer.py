from __future__ import annotations

import typing
from itertools import zip_longest

from prettytable import PrettyTable
from pyspark.sql.types import StructField, StructType

from chispa.formatting import blue


class SchemasNotEqualError(Exception):
    """The schemas are not equal"""

    pass


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
        t = PrettyTable(["schema1", "schema2"])
        zipped = list(zip_longest(s1, s2))
        for sf1, sf2 in zipped:
            if are_structfields_equal(sf1, sf2, True):
                t.add_row([blue(str(sf1)), blue(str(sf2))])
            else:
                t.add_row([sf1, sf2])
        raise SchemasNotEqualError("\n" + t.get_string())


# deprecate this
# perhaps it is a little faster, but do we really need this?
# I think schema equality operations are really fast to begin with
def assert_basic_schema_equality(s1: StructType, s2: StructType) -> None:
    if s1 != s2:
        t = PrettyTable(["schema1", "schema2"])
        zipped = list(zip_longest(s1, s2))
        for sf1, sf2 in zipped:
            if sf1 == sf2:
                t.add_row([blue(str(sf1)), blue(str(sf2))])
            else:
                t.add_row([sf1, sf2])
        raise SchemasNotEqualError("\n" + t.get_string())


# deprecate this.  ignore_nullable should be a flag.
def assert_schema_equality_ignore_nullable(s1: StructType, s2: StructType) -> None:
    if not are_schemas_equal_ignore_nullable(s1, s2):
        t = PrettyTable(["schema1", "schema2"])
        zipped = list(zip_longest(s1, s2))
        for sf1, sf2 in zipped:
            if are_structfields_equal(sf1, sf2, True):
                t.add_row([blue(str(sf1)), blue(str(sf2))])
            else:
                t.add_row([sf1, sf2])
        raise SchemasNotEqualError("\n" + t.get_string())


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
            return are_datatypes_equal_ignore_nullable(sf1.dataType, sf2.dataType)  # type: ignore[no-any-return, no-untyped-call]


# deprecate this
@typing.no_type_check
def are_datatypes_equal_ignore_nullable(dt1, dt2) -> bool:
    """Checks if datatypes are equal, descending into structs and arrays to
    ignore nullability.
    """
    if dt1.typeName() == dt2.typeName():
        # Account for array types by inspecting elementType.
        if dt1.typeName() == "array":
            return are_datatypes_equal_ignore_nullable(dt1.elementType, dt2.elementType)
        elif dt1.typeName() == "struct":
            return are_schemas_equal_ignore_nullable(dt1, dt2, ignore_metadata)
        else:
            return True
    else:
        return False
