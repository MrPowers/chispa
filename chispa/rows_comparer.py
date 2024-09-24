from __future__ import annotations

from itertools import zip_longest
from typing import Any, Callable

from prettytable import PrettyTable
from pyspark.sql import Row

import chispa
from chispa.formatting import FormattingConfig, format_string


def assert_basic_rows_equality(
    rows1: list[Row], rows2: list[Row], underline_cells: bool = False, formats: FormattingConfig | None = None
) -> None:
    if not formats:
        formats = FormattingConfig()
    elif not isinstance(formats, FormattingConfig):
        formats = FormattingConfig._from_arbitrary_dataclass(formats)

    if rows1 != rows2:
        t = PrettyTable(["df1", "df2"])
        zipped = list(zip_longest(rows1, rows2))
        all_rows_equal = True

        for r1, r2 in zipped:
            if r1 is None and r2 is not None:
                t.add_row([None, format_string(str(r2), formats.mismatched_rows)])
                all_rows_equal = False
            elif r1 is not None and r2 is None:
                t.add_row([format_string(str(r1), formats.mismatched_rows), None])
                all_rows_equal = False
            else:
                r_zipped = list(zip_longest(r1.__fields__, r2.__fields__))
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
        if all_rows_equal is False:
            raise chispa.DataFramesNotEqualError("\n" + t.get_string())


def assert_generic_rows_equality(
    rows1: list[Row],
    rows2: list[Row],
    row_equality_fun: Callable,  # type: ignore[type-arg]
    row_equality_fun_args: dict[str, Any],
    underline_cells: bool = False,
    formats: FormattingConfig | None = None,
) -> None:
    if not formats:
        formats = FormattingConfig()
    elif not isinstance(formats, FormattingConfig):
        formats = FormattingConfig._from_arbitrary_dataclass(formats)

    df1_rows = rows1
    df2_rows = rows2
    zipped = list(zip_longest(df1_rows, df2_rows))
    t = PrettyTable(["df1", "df2"])
    all_rows_equal = True
    for r1, r2 in zipped:
        # rows are not equal when one is None and the other isn't
        if (r1 is None) ^ (r2 is None):
            all_rows_equal = False
            t.add_row([
                format_string(str(r1), formats.mismatched_rows),
                format_string(str(r2), formats.mismatched_rows),
            ])
        # rows are equal
        elif row_equality_fun(r1, r2, **row_equality_fun_args):
            r1_string = ", ".join(map(lambda f: f"{f}={r1[f]}", r1.__fields__))
            r2_string = ", ".join(map(lambda f: f"{f}={r2[f]}", r2.__fields__))
            t.add_row([
                format_string(r1_string, formats.matched_rows),
                format_string(r2_string, formats.matched_rows),
            ])
        # otherwise, rows aren't equal
        else:
            r_zipped = list(zip_longest(r1.__fields__, r2.__fields__))
            r1_string_list: list[str] = []
            r2_string_list: list[str] = []
            for r1_field, r2_field in r_zipped:
                if r1[r1_field] != r2[r2_field]:
                    all_rows_equal = False
                    r1_string_list.append(format_string(f"{r1_field}={r1[r1_field]}", formats.mismatched_cells))
                    r2_string_list.append(format_string(f"{r2_field}={r2[r2_field]}", formats.mismatched_cells))
                else:
                    r1_string_list.append(format_string(f"{r1_field}={r1[r1_field]}", formats.matched_cells))
                    r2_string_list.append(format_string(f"{r2_field}={r2[r2_field]}", formats.matched_cells))
            r1_res = ", ".join(r1_string_list)
            r2_res = ", ".join(r2_string_list)

            t.add_row([r1_res, r2_res])
    if all_rows_equal is False:
        raise chispa.DataFramesNotEqualError("\n" + t.get_string())
