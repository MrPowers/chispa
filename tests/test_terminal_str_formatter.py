import pytest

from chispa.terminal_str_formatter import format_string


def test_it_can_make_a_blue_string():
    print(format_string("hi", ["bold", "blink"]))
