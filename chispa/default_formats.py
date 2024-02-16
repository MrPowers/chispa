from dataclasses import dataclass

@dataclass
class DefaultFormats:
    mismatched_rows = ["red", "bold"]
    matched_rows = ["blue"]
    mismatched_cells = ["white", "underline"]
    matched_cells = ["blue", "bold"]
