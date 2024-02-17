import pytest

from chispa.terminal_str_formatter import format_string


def test_it_can_make_a_blue_string():
    print(format_string("hi", ["bold", "blink"]))


def test_it_works_with_no_formats():
    print(format_string("hi", []))
