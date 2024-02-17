import pytest
from dataclasses import dataclass

@dataclass
class MyFormats:
    mismatched_rows = ["light_yellow"]
    matched_rows = ["cyan", "bold"]
    mismatched_cells = ["purple"]
    matched_cells = ["blue"]

@pytest.fixture()
def my_formats():
    return MyFormats()
