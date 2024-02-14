import chispa.six as six
from chispa.prettytable import PrettyTable
from chispa.bcolors import *
from chispa.terminal_str_formatter import format_string, format_mismatched_cell
import chispa
from pyspark.sql.types import Row
from typing import List


# {
#   "mismatched_rows": ["red", "bold"],
#   "matched_rows": "blue",
#   "mismatched_cells": ["white", "underline"],
#   "matched_cells": ["blue", "bold"]
# }


def assert_basic_rows_equality(rows1, rows2, underline_cells=False, formats={
  "mismatched_rows": ["red", "bold"],
  "matched_rows": ["blue"],
  "mismatched_cells": ["white", "underline"],
  "matched_cells": ["blue", "bold"]
}):
    if rows1 != rows2:
        t = PrettyTable(["df1", "df2"])
        zipped = list(six.moves.zip_longest(rows1, rows2))
        for r1, r2 in zipped:
            if r1 == r2:
                t.add_row([format_string(r1, formats["matched_rows"]), format_string(r2, formats["matched_rows"])])
            else:
                r = detailed_row_formatter(r1=r1, r2=r2, formats=formats)
                t.add_row(r)
        raise chispa.DataFramesNotEqualError("\n" + t.get_string())


def assert_generic_rows_equality(rows1, rows2, row_equality_fun, row_equality_fun_args, underline_cells=False):
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
            first = bcolors.LightBlue + str(r1) + bcolors.LightRed
            second = bcolors.LightBlue + str(r2) + bcolors.LightRed
            t.add_row([first, second])
        # otherwise, rows aren't equal
        else:
            allRowsEqual = False
            # Underline cells if requested
            if underline_cells:
                t.add_row(__underline_cells_in_row(
                    r1=r1, r2=r2, row_column_names=row_column_names, num_columns=num_columns))
            else:
                t.add_row([r1, r2])
    if allRowsEqual == False:
        raise chispa.DataFramesNotEqualError("\n" + t.get_string())


def __underline_cells_in_row(r1=Row, r2=Row, row_column_names=List[str], num_columns=int) -> List[str]:
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
            r1_string += underline_text(f"{column}='{r1[column]}'") + f"{append_str}"
            r2_string += underline_text(f"{column}='{r2[column]}'") + f"{append_str}"
        else:
            r1_string += f"{column}='{r1[column]}'{append_str}"
            r2_string += f"{column}='{r2[column]}'{append_str}"

    r1_string += ")"
    r2_string += ")"

    return [bcolors.LightRed + r1_string, r2_string]


def detailed_row_formatter(
        r1, 
        r2, 
        formats) -> List[str]:
    # row1_column_names = r1[0].__fields__
    # row2_column_names = r1[0].__fields__
    # num_columns1 = len(row1_column_names)
    # num_columns2 = len(row2_column_names)
    r1_string = ""
    r2_string = ""
    has_a_mismatched_cell = False
    zipped = list(six.moves.zip_longest(row1_column_names, row2_column_names))
    for column in zipped:
    # for index, column in enumerate(row_column_names):
    #     if ((index+1) == num_columns):
    #         append_str = ""
    #     else:
    #         append_str = ", "
        append_str = "&"

        if r1[column] != r2[column]:
            has_a_mismatched_cell = True
            r1_string += format_string(f"{column}='{r1[column]}'", formats["mismatched_cells"]) + f"{append_str}"
            r2_string += format_string(f"{column}='{r2[column]}'", formats["mismatched_cells"]) + f"{append_str}"
        else:
            r1_string += format_string(f"{column}='{r1[column]}'{append_str}", formats["matched_cells"])
            r2_string += format_string(f"{column}='{r2[column]}'{append_str}", formats["matched_cells"])

    if has_a_mismatched_cell:
        r1_string = format_string("Row(", formats["mismatched_rows"]) + r1_string + format_string(")", formats["mismatched_rows"])
        r2_string = format_string("Row(", formats["mismatched_rows"]) + r2_string + format_string(")", formats["mismatched_rows"])
    else:        
        r1_string = format_string("Row(", formats["matched_rows"]) + r1_string + format_string(")", formats["matched_rows"])
        r2_string = format_string("Row(", formats["matched_rows"]) + r2_string + format_string(")", formats["matched_rows"])

    return [r1_string, r2_string]
