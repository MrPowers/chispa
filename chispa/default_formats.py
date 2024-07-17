from __future__ import annotations

import warnings
from dataclasses import dataclass, field

from chispa.formatting import Color, Format, Style


@dataclass
class DefaultFormats:
    """
    This class is now deprecated. For backwards compatibility, when it's used, it will try to match the
    FormattingConfig class.
    """

    mismatched_rows: list[str] = field(default_factory=lambda: ["red"])
    matched_rows: list[str] = field(default_factory=lambda: ["blue"])
    mismatched_cells: list[str] = field(default_factory=lambda: ["red", "underline"])
    matched_cells: list[str] = field(default_factory=lambda: ["blue"])

    def __post_init__(self):
        warnings.warn(
            "DefaultFormats is deprecated. Use `chispa.formatting.FormattingConfig` instead.", DeprecationWarning
        )
        self.mismatched_rows = self._convert_to_format(self.mismatched_rows)
        self.matched_rows = self._convert_to_format(self.matched_rows)
        self.mismatched_cells = self._convert_to_format(self.mismatched_cells)
        self.matched_cells = self._convert_to_format(self.matched_cells)

    def _convert_to_format(self, values: list[str]) -> Format:
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

        return Format(color=color, style=styles if styles else None)
