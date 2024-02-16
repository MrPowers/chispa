from dataclasses import dataclass

@dataclass
class DefaultFormats:
    mismatched_rows = ["red"]
    matched_rows = ["blue"]
    mismatched_cells = ["white", "underline"]
    matched_cells = ["blue"]
