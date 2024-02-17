from dataclasses import dataclass

@dataclass
class DefaultFormats:
    mismatched_rows = ["red"]
    matched_rows = ["blue"]
    mismatched_cells = ["red", "underline"]
    matched_cells = ["blue"]
