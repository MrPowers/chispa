from prettytable import PrettyTable
from chispa.bcolors import *
import pyspark

class DataFramesNotEqualError(Exception):
   """The DataFrames are not equal"""
   pass


class SchemasNotEqualError(Exception):
   """The DataFrames are not equal"""
   pass


def assert_schema_equality(df1, df2):
    s1 = df1.schema
    s2 = df2.schema
    if s1 != s2:
        t = PrettyTable(["schema1", "schema2"])
        zipped = list(zip(s1, s2))
        for elements in zipped:
            t.add_row([elements[0], elements[1]])
        raise SchemasNotEqualError("\n" + t.get_string())


def assert_df_equality(df1, df2):
    assert_schema_equality(df1, df2)

    df1e = df1.collect()
    # df1e = list(map(lambda r: r.asDict().values(), df1.collect()))
    df2e = df2.collect()
    # df2e = list(map(lambda r: r.asDict().values(), df2.collect()))
    if df1e != df2e:
        t = PrettyTable(["df1", "df2"])
        zipped = list(zip(df1e, df2e))
        for elements in zipped:
            t.add_row([elements[0], elements[1]])
        raise DataFramesNotEqualError("\n" + t.get_string())


def are_dfs_equal(df1, df2):
    if df1.schema != df2.schema:
        return False
    if df1.collect() != df2.collect():
        return False
    return True


def are_rows_equal(r1: pyspark.sql.Row, r2: pyspark.sql.Row) -> bool:
    return r1 == r2


def are_rows_approx_equal(r1: pyspark.sql.Row, r2: pyspark.sql.Row, precision: float) -> bool:
    d1 = r1.asDict()
    d2 = r2.asDict()
    allEqual = True
    for key in d1.keys() & d2.keys():
        if isinstance(d1[key], float) and isinstance(d2[key], float):
            if abs(d1[key] - d2[key]) > precision:
                allEqual = False
        elif d1[key] != d2[key]:
            allEqual = False
    return allEqual


def assert_approx_df_equality(df1, df2, precision):
    assert_schema_equality(df1, df2)

    df1_rows = df1.collect()
    df2_rows = df2.collect()

    zipped = list(zip(df1_rows, df2_rows))
    t = PrettyTable(["df1", "df2"])
    allRowsEqual = True
    for r1, r2 in zipped:
        if are_rows_approx_equal(r1, r2, precision):
            first = bcolors.LightBlue + str(r1) + bcolors.LightRed
            second = bcolors.LightBlue + str(r2) + bcolors.LightRed
            t.add_row([first, second])
        else:
            allRowsEqual = False
            t.add_row([r1, r2])

    if allRowsEqual == False:
        raise DataFramesNotEqualError("\n" + t.get_string())

