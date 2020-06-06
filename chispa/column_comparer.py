from chispa.bcolors import *
from prettytable import PrettyTable


class ColumnsNotEqualError(Exception):
   """The columns are not equal"""
   pass


def assert_column_equality(df, col_name1, col_name2):
    elements = df.select(col_name1, col_name2).collect()
    colName1Elements = list(map(lambda x: x[0], elements))
    colName2Elements = list(map(lambda x: x[1], elements))
    if colName1Elements != colName2Elements:
        zipped = list(zip(colName1Elements, colName2Elements))
        t = PrettyTable([col_name1, col_name2])
        for elements in zipped:
            if elements[0] == elements[1]:
                first = bcolors.LightBlue + str(elements[0]) + bcolors.LightRed
                second = bcolors.LightBlue + str(elements[1]) + bcolors.LightRed
                t.add_row([first, second])
            else:
                t.add_row([str(elements[0]), str(elements[1])])
        raise ColumnsNotEqualError("\n" + t.get_string())
