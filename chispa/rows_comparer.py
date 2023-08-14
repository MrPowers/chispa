import chispa.six as six
from chispa.prettytable import PrettyTable
from chispa.bcolors import *
import chispa
from pyspark.sql.types import Row
from typing import List


def assert_basic_rows_equality(rows1, rows2, color_scheme, underline_cells=False):
    if underline_cells:
        row_column_names = rows1[0].__fields__
        num_columns = len(row_column_names)
    if rows1 != rows2:
        t = PrettyTable(["df1", "df2"])
        zipped = list(six.moves.zip_longest(rows1, rows2))
        for r1, r2 in zipped:
            if r1 == r2:
                t.add_row([normal_text(input_text=str(r1), color_scheme=color_scheme), normal_text(input_text=str(r2), color_scheme=color_scheme)])
            else:
                if underline_cells:
                    t.add_row(__underline_cells_in_row(
                        r1=r1, r2=r2, row_column_names=row_column_names, num_columns=num_columns, color_scheme=color_scheme))
                else:
                    t.add_row([r1, r2])
        raise chispa.DataFramesNotEqualError("\n" + t.get_string())


def assert_generic_rows_equality(rows1, rows2, row_equality_fun, row_equality_fun_args, color_scheme, underline_cells=False):
    df1_rows = rows1
    df2_rows = rows2
    zipped = list(six.moves.zip_longest(df1_rows, df2_rows))
    t = PrettyTable(["df1", "df2"])
    allRowsEqual = True
    if underline_cells:
        row_column_names = rows1[0].__fields__
        num_columns = len(row_column_names)
    for r1, r2 in zipped:
        # rows are not equal when one is None and the other isn't
        if (r1 is not None and r2 is None) or (r2 is not None and r1 is None):
            allRowsEqual = False
            t.add_row([r1, r2])
        # rows are equal
        elif row_equality_fun(r1, r2, *row_equality_fun_args):
            first = get_color(color_scheme["matched"]) + str(r1) + get_color(color_scheme["default"])
            second = get_color(color_scheme["matched"]) + str(r2) + get_color(color_scheme["default"])
            t.add_row([first, second])
        # otherwise, rows aren't equal
        else:
            allRowsEqual = False
            # Underline cells if requested
            if underline_cells:
                t.add_row(__underline_cells_in_row(
                    r1=r1, r2=r2, row_column_names=row_column_names, num_columns=num_columns, color_scheme=color_scheme))
            else:
                t.add_row([r1, r2])
    if allRowsEqual == False:
        raise chispa.DataFramesNotEqualError("\n" + t.get_string())


def __underline_cells_in_row(r1:Row, r2:Row, row_column_names:List[str], num_columns:int, color_scheme:dict) -> List[str]:
    """
    Takes two Row types, a list of column names for the Rows and the length of columns
    Returns list of two strings, with underlined columns within rows that are different for PrettyTable
    """
    r1_string = "Row("
    r2_string = "Row("
    for index, column in enumerate(row_column_names):
        if ((index+1) == num_columns):
            append_str = ""
        else:
            append_str = ", "

        if r1[column] != r2[column]:
            r1_string += underline_text(
                f"{column}='{r1[column]}'", color_scheme=color_scheme) + f"{append_str}"
            r2_string += underline_text(
                f"{column}='{r2[column]}'", color_scheme=color_scheme) + f"{append_str}"
        else:
            r1_string += f"{column}='{r1[column]}'{append_str}"
            r2_string += f"{column}='{r2[column]}'{append_str}"

    r1_string += ")"
    r2_string += ")"

    return [r1_string, r2_string]
