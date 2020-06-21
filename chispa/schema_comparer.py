from prettytable import PrettyTable
from chispa.bcolors import *


class SchemasNotEqualError(Exception):
   """The DataFrames are not equal"""
   pass


def assert_schema_equality(df1, df2):
    s1 = df1.schema
    s2 = df2.schema
    if s1 != s2:
        t = PrettyTable(["schema1", "schema2"])
        zipped = list(zip(s1, s2))
        for sf1, sf2 in zipped:
            if sf1 == sf2:
                t.add_row([blue(sf1), blue(sf2)])
            else:
                t.add_row([sf1, sf2])
        raise SchemasNotEqualError("\n" + t.get_string())


def assert_schema_equality_ignore_nullable(df1, df2):
    s1 = df1.schema
    s2 = df2.schema
    zipped = list(zip(s1, s2))
    if are_schemas_equal_ignore_nullable(s1, s2) == False:
        t = PrettyTable(["schema1", "schema2"])
        zipped = list(zip(s1, s2))
        for sf1, sf2 in zipped:
            if sf1 == sf2:
                t.add_row([blue(sf1), blue(sf2)])
            else:
                t.add_row([sf1, sf2])
        raise SchemasNotEqualError("\n" + t.get_string())


def are_schemas_equal_ignore_nullable(s1, s2):
    if len(s1) != len(s2):
        return False
    zipped = list(zip(s1, s2))
    for sf1, sf2 in zipped:
        if sf1.name != sf2.name or sf1.dataType != sf2.dataType:
          return False
    return True

