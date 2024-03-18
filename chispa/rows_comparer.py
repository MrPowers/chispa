import chispa.six as six
from chispa.prettytable import PrettyTable
from chispa.bcolors import *
import chispa
from pyspark.sql.types import Row
from typing import List
from chispa.terminal_str_formatter import format_string
from chispa.default_formats import DefaultFormats
import itertools


def assert_basic_rows_equality(rows1, rows2, underline_cells=False, formats=DefaultFormats(), output_format="two_columns"):
    if rows1 != rows2:
        zipped = list(six.moves.zip_longest(rows1, rows2))
        if output_format == "two_columns":
            t = PrettyTable(["df1", "df2"])
        elif output_format == "smush":
            t = PrettyTable(rows1[0].__fields__)
        all_rows_equal = True
        for r1, r2 in zipped:
            if output_format == "two_columns":
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
            elif output_format == "smush":
                if r1 is None and r2 is not None:
                    tmp = []
                    for f in r2.__fields__:
                        tmp.append(format_string("None" + " | " + f, formats.mismatched_cells))
                    t.add_row(tmp)
                    all_rows_equal = False
                elif r1 is not None and r2 is None:
                    tmp = []
                    for f in r1.__fields__:
                        tmp.append(format_string(f + " | " + "None", formats.mismatched_cells))
                    t.add_row(tmp)
                    all_rows_equal = False
                else:
                    res = []
                    zipped_row = itertools.zip_longest(r1, r2)
                    for cell1, cell2 in zipped_row:
                        if cell1 == cell2:
                            res.append(format_string(cell1, formats.matched_cells) + " | " + format_string(cell2, formats.matched_cells))
                        else:
                            res.append(format_string(cell1, formats.mismatched_cells) + " | " + format_string(cell2, formats.mismatched_cells))
                            all_rows_equal = False
                    t.add_row(res)

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


class Mesita:
    def __init__(self, field_names, rows1, rows2, match_formats, mismatch_formats, equality_fun):
        self.field_names = field_names
        self.rows1 = rows1
        self.rows2 = rows2
        self.match_formats = match_formats
        self.mismatch_formats = mismatch_formats
        self.equality_fun = equality_fun
    
    def as_columns(self):
        t = PrettyTable(["df1", "df2"])
        zipped = itertools.zip_longest(self.rows1, self.rows2)
        for r1, r2 in zipped:
            res1 = []
            res2 = []
            zipped_row = itertools.zip_longest(r1, r2, self.field_names)
            for cell1, cell2, field_name in zipped_row:
                if self.equality_fun(cell1, cell2):
                    res1.append(f"{field_name}=" + format_string(cell1, self.match_formats))
                    res2.append(f"{field_name}=" + format_string(cell2, self.match_formats))
                else:
                    res1.append(f"{field_name}=" + format_string(cell1, self.mismatch_formats))
                    res2.append(f"{field_name}=" + format_string(cell2, self.mismatch_formats))
            t.add_row([", ".join(res1), ", ".join(res2)])
        return t.get_string()

    def side_by_side(self):
        field_names1 = self.field_names
        field_names2 = list(map(lambda n: f"{n}_2", self.field_names))
        t = PrettyTable(field_names1 + ["|"] + field_names2)
        zipped = itertools.zip_longest(self.rows1, self.rows2)
        for r1, r2 in zipped:
            res1 = []
            res2 = []
            zipped_row = itertools.zip_longest(r1, r2)
            for cell1, cell2 in zipped_row:
                if self.equality_fun(cell1, cell2):
                    res1.append(format_string(cell1, self.match_formats))
                    res2.append(format_string(cell2, self.match_formats))
                else:
                    res1.append(format_string(cell1, self.mismatch_formats))
                    res2.append(format_string(cell2, self.mismatch_formats))
            t.add_row(res1 + ["|"] + res2)
        return t.get_string()

    def smush(self):
        t = PrettyTable(self.field_names)
        zipped = itertools.zip_longest(self.rows1, self.rows2)
        for r1, r2 in zipped:
            res = []
            zipped_row = itertools.zip_longest(r1, r2)
            for cell1, cell2 in zipped_row:
                if self.equality_fun(cell1, cell2):
                    res.append(format_string(cell1, self.match_formats) + " | " + format_string(cell2, self.match_formats))
                else:
                    res.append(format_string(cell1, self.mismatch_formats) + " | " + format_string(cell2, self.mismatch_formats))
            t.add_row(res)
        return t.get_string()