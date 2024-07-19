from __future__ import annotations

from chispa.formatting import RESET, format_string
from chispa.formatting.formats import Color, Format, Style


def test_format_with_enum_inputs():
    format = Format(color=Color.BLUE, style=[Style.BOLD, Style.UNDERLINE])
    formatted_string = format_string("Hello, World!", format)
    expected_string = f"{Style.BOLD.value}{Style.UNDERLINE.value}{Color.BLUE.value}Hello, World!{RESET}"
    assert formatted_string == expected_string


def test_format_with_no_style():
    format = Format(color=Color.GREEN, style=[])
    formatted_string = format_string("Hello, World!", format)
    expected_string = f"{Color.GREEN.value}Hello, World!{RESET}"
    assert formatted_string == expected_string


def test_format_with_no_color():
    format = Format(color=None, style=[Style.BLINK])
    formatted_string = format_string("Hello, World!", format)
    expected_string = f"{Style.BLINK.value}Hello, World!{RESET}"
    assert formatted_string == expected_string


def test_format_with_no_color_or_style():
    format = Format(color=None, style=[])
    formatted_string = format_string("Hello, World!", format)
    expected_string = "Hello, World!"
    assert formatted_string == expected_string
