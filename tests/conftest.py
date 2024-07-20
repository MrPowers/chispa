from __future__ import annotations

import pytest

from chispa.formatting import FormattingConfig


@pytest.fixture()
def my_formats():
    return FormattingConfig(
        mismatched_rows={"color": "light_yellow"},
        matched_rows={"color": "cyan", "style": "bold"},
        mismatched_cells={"color": "purple"},
        matched_cells={"color": "blue"},
    )


@pytest.fixture()
def my_chispa():
    return FormattingConfig(
        mismatched_rows={"color": "light_yellow"},
        matched_rows={"color": "cyan", "style": "bold"},
        mismatched_cells={"color": "purple"},
        matched_cells={"color": "blue"},
    )
