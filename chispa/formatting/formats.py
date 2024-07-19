from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import ClassVar

RESET = "\033[0m"


class Color(Enum):
    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    PURPLE = "\033[35m"
    CYAN = "\033[36m"
    LIGHT_GRAY = "\033[37m"
    DARK_GRAY = "\033[90m"
    LIGHT_RED = "\033[91m"
    LIGHT_GREEN = "\033[92m"
    LIGHT_YELLOW = "\033[93m"
    LIGHT_BLUE = "\033[94m"
    LIGHT_PURPLE = "\033[95m"
    LIGHT_CYAN = "\033[96m"
    WHITE = "\033[97m"


class Style(Enum):
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"
    BLINK = "\033[5m"
    INVERT = "\033[7m"
    HIDE = "\033[8m"


@dataclass
class Format:
    color: Color | None = None
    style: list[Style] | None = None

    @classmethod
    def from_dict(cls, format_dict: dict) -> Format:
        if not isinstance(format_dict, dict):
            raise ValueError("Input must be a dictionary")

        color = cls._get_color_enum(format_dict.get("color"))
        style = format_dict.get("style")
        if isinstance(style, str):
            styles = [cls._get_style_enum(style)]
        elif isinstance(style, list):
            styles = [cls._get_style_enum(s) for s in style]
        else:
            styles = None

        return cls(color=color, style=styles)

    @staticmethod
    def _get_color_enum(color: Color | str | None) -> Color | None:
        if isinstance(color, Color):
            return color
        elif isinstance(color, str):
            try:
                return Color[color.upper()]
            except KeyError:
                valid_colors = [c.name.lower() for c in Color]
                raise ValueError(f"Invalid color name: {color}. Valid color names are {valid_colors}")
        return None

    @staticmethod
    def _get_style_enum(style: Style | str | None) -> Style | None:
        if isinstance(style, Style):
            return style
        elif isinstance(style, str):
            try:
                return Style[style.upper()]
            except KeyError:
                valid_styles = [f.name.lower() for f in Style]
                raise ValueError(f"Invalid style name: {style}. Valid style names are {valid_styles}")
        return None


class FormattingConfig:
    """
    Class to manage and parse formatting configurations.
    """

    VALID_KEYS: ClassVar = {"color", "style"}

    def __init__(
        self,
        mismatched_rows: Format | dict = Format(Color.RED),
        matched_rows: Format | dict = Format(Color.BLUE),
        mismatched_cells: Format | dict = Format(Color.RED, [Style.UNDERLINE]),
        matched_cells: Format | dict = Format(Color.BLUE),
    ):
        """
        Initializes the FormattingConfig with given or default formatting.

        Each of the arguments can be provided as a `Format` object or a dictionary with the following keys:
        - 'color': A string representing a color name, which should be one of the valid colors:
            ['black', 'red', 'green', 'yellow', 'blue', 'purple', 'cyan', 'light_gray',
            'dark_gray', 'light_red', 'light_green', 'light_yellow', 'light_blue',
            'light_purple', 'light_cyan', 'white'].
        - 'style': A string or list of strings representing styles, which should be one of the valid styles:
            ['bold', 'underline', 'blink', 'invert', 'hide'].

        Args:
            mismatched_rows (Format | dict): Format or dictionary for mismatched rows.
            matched_rows (Format | dict): Format or dictionary for matched rows.
            mismatched_cells (Format | dict): Format or dictionary for mismatched cells.
            matched_cells (Format | dict): Format or dictionary for matched cells.

        Raises:
            ValueError: If the dictionary contains invalid keys or values.
        """
        self.mismatched_rows: Format = self._parse_format(mismatched_rows)
        self.matched_rows: Format = self._parse_format(matched_rows)
        self.mismatched_cells: Format = self._parse_format(mismatched_cells)
        self.matched_cells: Format = self._parse_format(matched_cells)

    def _parse_format(self, format: Format | dict) -> Format:
        if isinstance(format, Format):
            return format
        elif isinstance(format, dict):
            return Format.from_dict(format)
        raise ValueError("Invalid format type. Must be Format or dict.")
