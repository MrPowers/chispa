from __future__ import annotations

import re

import pytest

from chispa.formatting import Color, FormattingConfig, Style


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
    with pytest.raises(ValueError) as exc_info:
        FormattingConfig(mismatched_rows={"color": "invalid_color"})
    assert str(exc_info.value) == (
        "Invalid color name: invalid_color. Valid color names are "
        "['black', 'red', 'green', 'yellow', 'blue', 'purple', 'cyan', 'light_gray', "
        "'dark_gray', 'light_red', 'light_green', 'light_yellow', 'light_blue', 'light_purple', "
        "'light_cyan', 'white']"
    )


def test_invalid_style():
    with pytest.raises(ValueError) as exc_info:
        FormattingConfig(mismatched_rows={"style": ["invalid_style"]})
    assert str(exc_info.value) == (
        "Invalid style name: invalid_style. Valid style names are " "['bold', 'underline', 'blink', 'invert', 'hide']"
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
