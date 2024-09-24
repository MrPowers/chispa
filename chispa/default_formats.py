from __future__ import annotations

import warnings
from dataclasses import dataclass, field


@dataclass
class DefaultFormats:
    """
    This class is now deprecated and should be removed in a future release.
    """

    mismatched_rows: list[str] = field(default_factory=lambda: ["red"])
    matched_rows: list[str] = field(default_factory=lambda: ["blue"])
    mismatched_cells: list[str] = field(default_factory=lambda: ["red", "underline"])
    matched_cells: list[str] = field(default_factory=lambda: ["blue"])

    def __post_init__(self) -> None:
        warnings.warn(
            "DefaultFormats is deprecated. Use `chispa.formatting.FormattingConfig` instead.", DeprecationWarning
        )
