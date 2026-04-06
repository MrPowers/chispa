from __future__ import annotations

import os

import pytest
from pyspark.sql import SparkSession

from chispa.formatting import FormattingConfig


@pytest.fixture(scope="session")
def spark():
    os.environ["SPARK_TESTING"] = "1"
    return SparkSession.builder.master("local").appName("chispa").getOrCreate()


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
