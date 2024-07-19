from __future__ import annotations

import re

from chispa.formatting.formats import Color, Format, FormattingConfig, Style


def test_default_mismatched_rows():
    config = FormattingConfig()
    assert config.mismatched_rows.color == Color.RED
    assert config.mismatched_rows.style is None


def test_default_matched_rows():
    config = FormattingConfig()
    assert config.matched_rows.color == Color.BLUE
    assert config.matched_rows.style is None


def test_default_mismatched_cells():
    config = FormattingConfig()
    assert config.mismatched_cells.color == Color.RED
    assert config.mismatched_cells.style == [Style.UNDERLINE]


def test_default_matched_cells():
    config = FormattingConfig()
    assert config.matched_cells.color == Color.BLUE
    assert config.matched_cells.style is None


def test_custom_mismatched_rows():
    config = FormattingConfig(mismatched_rows={"color": "green", "style": ["bold", "underline"]})
    assert config.mismatched_rows.color == Color.GREEN
    assert config.mismatched_rows.style == [Style.BOLD, Style.UNDERLINE]


def test_custom_matched_rows():
    config = FormattingConfig(matched_rows={"color": "yellow"})
    assert config.matched_rows.color == Color.YELLOW
    assert config.matched_rows.style is None


def test_custom_mismatched_cells():
    config = FormattingConfig(mismatched_cells={"color": "purple", "style": ["blink"]})
    assert config.mismatched_cells.color == Color.PURPLE
    assert config.mismatched_cells.style == [Style.BLINK]


def test_custom_matched_cells():
    config = FormattingConfig(matched_cells={"color": "cyan", "style": ["invert", "hide"]})
    assert config.matched_cells.color == Color.CYAN
    assert config.matched_cells.style == [Style.INVERT, Style.HIDE]


def test_invalid_color():
    try:
        FormattingConfig(mismatched_rows={"color": "invalid_color"})
    except ValueError as e:
        assert (
            str(e)
            == "Invalid color name: invalid_color. Valid color names are ['black', 'red', 'green', 'yellow', 'blue', 'purple', 'cyan', 'light_gray', 'dark_gray', 'light_red', 'light_green', 'light_yellow', 'light_blue', 'light_purple', 'light_cyan', 'white']"
        )


def test_invalid_style():
    try:
        FormattingConfig(mismatched_rows={"style": ["invalid_style"]})
    except ValueError as e:
        assert (
            str(e)
            == "Invalid style name: invalid_style. Valid style names are ['bold', 'underline', 'blink', 'invert', 'hide']"
        )


def test_invalid_key():
    try:
        FormattingConfig(mismatched_rows={"invalid_key": "value"})
    except ValueError as e:
        error_message = str(e)
        assert re.match(
            r"Invalid keys in format dictionary: \{'invalid_key'\}. Valid keys are \{('color', 'style'|'style', 'color')\}",
            error_message,
        )


def test_format_from_dict_valid():
    format_dict = {"color": "blue", "style": ["bold", "underline"]}
    format_instance = Format.from_dict(format_dict)
    assert format_instance.color == Color.BLUE
    assert format_instance.style == [Style.BOLD, Style.UNDERLINE]


def test_format_from_dict_invalid_color():
    format_dict = {"color": "invalid_color", "style": ["bold"]}
    try:
        Format.from_dict(format_dict)
    except ValueError as e:
        assert (
            str(e)
            == "Invalid color name: invalid_color. Valid color names are ['black', 'red', 'green', 'yellow', 'blue', 'purple', 'cyan', 'light_gray', 'dark_gray', 'light_red', 'light_green', 'light_yellow', 'light_blue', 'light_purple', 'light_cyan', 'white']"
        )


def test_format_from_dict_invalid_style():
    format_dict = {"color": "blue", "style": ["invalid_style"]}
    try:
        Format.from_dict(format_dict)
    except ValueError as e:
        assert (
            str(e)
            == "Invalid style name: invalid_style. Valid style names are ['bold', 'underline', 'blink', 'invert', 'hide']"
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
