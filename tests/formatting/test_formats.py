from __future__ import annotations

import re

import pytest

from chispa.formatting import Color, Format, Style


def test_format_from_dict_valid():
    format_dict = {"color": "blue", "style": ["bold", "underline"]}
    format_instance = Format.from_dict(format_dict)
    assert format_instance.color == Color.BLUE
    assert format_instance.style == [Style.BOLD, Style.UNDERLINE]


def test_format_from_dict_invalid_color():
    format_dict = {"color": "invalid_color", "style": ["bold"]}
    with pytest.raises(ValueError) as exc_info:
        Format.from_dict(format_dict)
    assert str(exc_info.value) == (
        "Invalid color name: invalid_color. Valid color names are "
        "['black', 'red', 'green', 'yellow', 'blue', 'purple', 'cyan', 'light_gray', "
        "'dark_gray', 'light_red', 'light_green', 'light_yellow', 'light_blue', 'light_purple', "
        "'light_cyan', 'white']"
    )


def test_format_from_dict_invalid_style():
    format_dict = {"color": "blue", "style": ["invalid_style"]}
    with pytest.raises(ValueError) as exc_info:
        Format.from_dict(format_dict)
    assert str(exc_info.value) == (
        "Invalid style name: invalid_style. Valid style names are " "['bold', 'underline', 'blink', 'invert', 'hide']"
    )


def test_format_from_dict_invalid_key():
    format_dict = {"invalid_key": "value"}
    try:
        Format.from_dict(format_dict)
    except ValueError as e:
        error_message = str(e)
        assert re.match(
            r"Invalid keys in format dictionary: \{'invalid_key'\}. Valid keys are \{('color', 'style'|'style', 'color')\}",
            error_message,
        )


def test_format_from_list_valid():
    values = ["blue", "bold", "underline"]
    format_instance = Format.from_list(values)
    assert format_instance.color == Color.BLUE
    assert format_instance.style == [Style.BOLD, Style.UNDERLINE]


def test_format_from_list_invalid_color():
    values = ["invalid_color", "bold"]
    with pytest.raises(ValueError) as exc_info:
        Format.from_list(values)
    assert str(exc_info.value) == (
        "Invalid value: invalid_color. Valid values are colors: "
        "['black', 'red', 'green', 'yellow', 'blue', 'purple', 'cyan', 'light_gray', "
        "'dark_gray', 'light_red', 'light_green', 'light_yellow', 'light_blue', 'light_purple', "
        "'light_cyan', 'white'] and styles: ['bold', 'underline', 'blink', 'invert', 'hide']"
    )


def test_format_from_list_invalid_style():
    values = ["blue", "invalid_style"]
    with pytest.raises(ValueError) as exc_info:
        Format.from_list(values)
    assert str(exc_info.value) == (
        "Invalid value: invalid_style. Valid values are colors: "
        "['black', 'red', 'green', 'yellow', 'blue', 'purple', 'cyan', 'light_gray', "
        "'dark_gray', 'light_red', 'light_green', 'light_yellow', 'light_blue', 'light_purple', "
        "'light_cyan', 'white'] and styles: ['bold', 'underline', 'blink', 'invert', 'hide']"
    )


def test_format_from_list_non_string_elements():
    values = ["blue", 123]
    with pytest.raises(ValueError) as exc_info:
        Format.from_list(values)
    assert str(exc_info.value) == "All elements in the list must be strings"


def test_format_from_dict_empty():
    format_dict = {}
    format_instance = Format.from_dict(format_dict)
    assert format_instance.color is None
    assert format_instance.style is None


def test_format_from_list_empty():
    values = []
    format_instance = Format.from_list(values)
    assert format_instance.color is None
    assert format_instance.style is None
