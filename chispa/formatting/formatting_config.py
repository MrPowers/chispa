from __future__ import annotations

import warnings
from typing import Any, ClassVar

from chispa.default_formats import DefaultFormats
from chispa.formatting.formats import Color, Format, Style


class FormattingConfig:
    """
    Class to manage and parse formatting configurations.
    """

    VALID_KEYS: ClassVar = {"color", "style"}

    def __init__(
        self,
        mismatched_rows: Format | dict[str, str | list[str]] = Format(Color.RED),
        matched_rows: Format | dict[str, str | list[str]] = Format(Color.BLUE),
        mismatched_cells: Format | dict[str, str | list[str]] = Format(Color.RED, [Style.UNDERLINE]),
        matched_cells: Format | dict[str, str | list[str]] = Format(Color.BLUE),
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

    def _parse_format(self, format: Format | dict[str, str | list[str]]) -> Format:
        if isinstance(format, Format):
            return format
        elif isinstance(format, dict):
            return Format.from_dict(format)
        raise ValueError("Invalid format type. Must be Format or dict.")

    @classmethod
    def _from_arbitrary_dataclass(cls, instance: Any) -> FormattingConfig:
        """
        Converts an instance of an arbitrary class with specified fields to a FormattingConfig instance.
        This method is purely for backwards compatibility and should be removed in a future release,
        together with the `DefaultFormats` class.
        """

        if not isinstance(instance, DefaultFormats):
            warnings.warn(
                "Using an arbitrary dataclass is deprecated. Use `chispa.formatting.FormattingConfig` instead.",
                DeprecationWarning,
            )

        mismatched_rows = Format.from_list(getattr(instance, "mismatched_rows"))
        matched_rows = Format.from_list(getattr(instance, "matched_rows"))
        mismatched_cells = Format.from_list(getattr(instance, "mismatched_cells"))
        matched_cells = Format.from_list(getattr(instance, "matched_cells"))

        return cls(
            mismatched_rows=mismatched_rows,
            matched_rows=matched_rows,
            mismatched_cells=mismatched_cells,
            matched_cells=matched_cells,
        )
