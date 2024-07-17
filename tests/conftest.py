from dataclasses import dataclass

import pytest

from chispa import Chispa


@dataclass
class MyFormats:
    mismatched_rows = ["light_yellow"]
    matched_rows = ["cyan", "bold"]
    mismatched_cells = ["purple"]
    matched_cells = ["blue"]


@pytest.fixture()
def my_formats():
    return MyFormats()


@pytest.fixture()
def my_chispa():
    return Chispa(formats=MyFormats())
