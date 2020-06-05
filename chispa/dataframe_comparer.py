from prettytable import PrettyTable

class DataFramesNotEqualError(Exception):
   """The DataFrames are not equal"""
   pass


class SchemasNotEqualError(Exception):
   """The DataFrames are not equal"""
   pass


def assert_df_equality(df1, df2):
    if df1.schema != df2.schema:
        raise SchemasNotEqualError("These schemas aren't equal")

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

