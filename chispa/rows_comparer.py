import chispa.six as six
from chispa.prettytable import PrettyTable
from chispa.bcolors import *
import chispa
from pyspark.sql.types import Row
from typing import List
from chispa.terminal_str_formatter import format_string
from chispa.default_formats import DefaultFormats


def assert_basic_rows_equality(rows1, rows2, underline_cells=False, formats=DefaultFormats()):
    if rows1 != rows2:
        t = PrettyTable(["df1", "df2"])
        zipped = list(six.moves.zip_longest(rows1, rows2))
        all_rows_equal = True
        for r1, r2 in zipped:
            if r1 is None and r2 is not None:
                t.add_row([None, format_string(r2, formats.mismatched_rows)])
                all_rows_equal = False
            elif r1 is not None and r2 is None:
                t.add_row([format_string(r1, formats.mismatched_rows), None])
                all_rows_equal = False
            else:
                r_zipped = list(six.moves.zip_longest(r1.__fields__, r2.__fields__))
                r1_string = []
                r2_string = []
                for r1_field, r2_field in r_zipped:
                    if r1[r1_field] != r2[r2_field]:
                        all_rows_equal = False
                        r1_string.append(format_string(f"{r1_field}={r1[r1_field]}", formats.mismatched_cells))
                        r2_string.append(format_string(f"{r2_field}={r2[r2_field]}", formats.mismatched_cells))
                    else:
                        r1_string.append(format_string(f"{r1_field}={r1[r1_field]}", formats.matched_cells))
                        r2_string.append(format_string(f"{r2_field}={r2[r2_field]}", formats.matched_cells))
                r1_res = ", ".join(r1_string)
                r2_res = ", ".join(r2_string)

                t.add_row([r1_res, r2_res])
        if all_rows_equal == False:
            raise chispa.DataFramesNotEqualError("\n" + t.get_string())


def assert_generic_rows_equality(rows1, rows2, row_equality_fun, row_equality_fun_args, underline_cells=False, formats=DefaultFormats()):
    df1_rows = rows1
    df2_rows = rows2
    zipped = list(six.moves.zip_longest(df1_rows, df2_rows))
    t = PrettyTable(["df1", "df2"])
    all_rows_equal = True
    for r1, r2 in zipped:
        # rows are not equal when one is None and the other isn't
        if (r1 is not None and r2 is None) or (r2 is not None and r1 is None):
            all_rows_equal = False
            t.add_row([format_string(r1, formats.mismatched_rows), format_string(r2, formats.mismatched_rows)])
        # rows are equal
        elif row_equality_fun(r1, r2, *row_equality_fun_args):
            r1_string = ", ".join(map(lambda f: f"{f}={r1[f]}", r1.__fields__))
            r2_string = ", ".join(map(lambda f: f"{f}={r2[f]}", r2.__fields__))
            t.add_row([format_string(r1_string, formats.matched_rows), format_string(r2_string, formats.matched_rows)])
        # otherwise, rows aren't equal
        else:
            r_zipped = list(six.moves.zip_longest(r1.__fields__, r2.__fields__))
            r1_string = []
            r2_string = []
            for r1_field, r2_field in r_zipped:
                if r1[r1_field] != r2[r2_field]:
                    all_rows_equal = False
                    r1_string.append(format_string(f"{r1_field}={r1[r1_field]}", formats.mismatched_cells))
                    r2_string.append(format_string(f"{r2_field}={r2[r2_field]}", formats.mismatched_cells))
                else:
                    r1_string.append(format_string(f"{r1_field}={r1[r1_field]}", formats.matched_cells))
                    r2_string.append(format_string(f"{r2_field}={r2[r2_field]}", formats.matched_cells))
            r1_res = ", ".join(r1_string)
            r2_res = ", ".join(r2_string)

            t.add_row([r1_res, r2_res])
    if all_rows_equal == False:
        raise chispa.DataFramesNotEqualError("\n" + t.get_string())
