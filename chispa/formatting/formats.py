from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

RESET = "\033[0m"


class Color(str, Enum):
    """
    Enum for terminal colors.
    Each color is represented by its corresponding ANSI escape code.
    """

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


class Style(str, Enum):
    """
    Enum for text styles.
    Each style is represented by its corresponding ANSI escape code.
    """

    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"
    BLINK = "\033[5m"
    INVERT = "\033[7m"
    HIDE = "\033[8m"


@dataclass
class Format:
    """
    Data class to represent text formatting with color and style.

    Attributes:
        color (Color | None): The color for the text.
        style (list[Style] | None): A list of styles for the text.
    """

    color: Color | None = None
    style: list[Style] | None = None

    @classmethod
    def from_dict(cls, format_dict: dict[str, str | list[str]]) -> Format:
        """
        Create a Format instance from a dictionary.

        Args:
            format_dict (dict): A dictionary with keys 'color' and/or 'style'.
        """
        if not isinstance(format_dict, dict):
            raise ValueError("Input must be a dictionary")

        valid_keys = {"color", "style"}
        invalid_keys = set(format_dict) - valid_keys
        if invalid_keys:
            raise ValueError(f"Invalid keys in format dictionary: {invalid_keys}. Valid keys are {valid_keys}")

        if isinstance(format_dict.get("color"), list):
            raise TypeError("The value for key 'color' should be a string, not a list!")
        color = cls._get_color_enum(format_dict.get("color"))  # type: ignore[arg-type]

        style = format_dict.get("style")
        if isinstance(style, str):
            styles = [cls._get_style_enum(style)]
        elif isinstance(style, list):
            styles = [cls._get_style_enum(s) for s in style]
        else:
            styles = None

        return cls(color=color, style=styles)  # type: ignore[arg-type]

    @classmethod
    def from_list(cls, values: list[str]) -> Format:
        """
        Create a Format instance from a list of strings.

        Args:
            values (list[str]): A list of strings representing colors and styles.
        """
        if not all(isinstance(value, str) for value in values):
            raise ValueError("All elements in the list must be strings")

        color = None
        styles = []
        valid_colors = [c.name.lower() for c in Color]
        valid_styles = [s.name.lower() for s in Style]

        for value in values:
            if value in valid_colors:
                color = Color[value.upper()]
            elif value in valid_styles:
                styles.append(Style[value.upper()])
            else:
                raise ValueError(
                    f"Invalid value: {value}. Valid values are colors: {valid_colors} and styles: {valid_styles}"
                )

        return cls(color=color, style=styles if styles else None)

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
