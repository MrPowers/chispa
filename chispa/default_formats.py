from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from typing import Any

from chispa.formatting import Color, Format, FormattingConfig, Style


@dataclass
class DefaultFormats:
    """
    This class is now deprecated and should be removed in a future release, together with `convert_to_formatting_config`.
    """

    mismatched_rows: list[str] = field(default_factory=lambda: ["red"])
    matched_rows: list[str] = field(default_factory=lambda: ["blue"])
    mismatched_cells: list[str] = field(default_factory=lambda: ["red", "underline"])
    matched_cells: list[str] = field(default_factory=lambda: ["blue"])

    def __post_init__(self):
        warnings.warn(
            "DefaultFormats is deprecated. Use `chispa.formatting.FormattingConfig` instead.", DeprecationWarning
        )


def convert_to_formatting_config(instance: Any) -> FormattingConfig:
    """
    Converts an instance of an arbitrary class with specified fields to a FormattingConfig instance.
    This class is purely for backwards compatibility and should be removed in a future release.
    """

    if type(instance) is not DefaultFormats:
        warnings.warn(
            "Using an arbitrary dataclass is deprecated. Use `chispa.formatting.FormattingConfig` instead.",
            DeprecationWarning,
        )

    def _convert_to_format(values: list[str]) -> Format:
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

    mismatched_rows = _convert_to_format(getattr(instance, "mismatched_rows"))
    matched_rows = _convert_to_format(getattr(instance, "matched_rows"))
    mismatched_cells = _convert_to_format(getattr(instance, "mismatched_cells"))
    matched_cells = _convert_to_format(getattr(instance, "matched_cells"))

    return FormattingConfig(
        mismatched_rows=mismatched_rows,
        matched_rows=matched_rows,
        mismatched_cells=mismatched_cells,
        matched_cells=matched_cells,
    )
