import pytest

from chispa.dataframe_comparer import *
from chispa.column_comparer import *
import pyspark.sql.functions as F

def remove_non_word_characters(col):
    return F.regexp_replace(col, "[^\\w\\s]+", "")

from pyspark.sql import SparkSession

spark = SparkSession.builder \
  .master("local") \
  .appName("chispa") \
  .getOrCreate()

def test_remove_non_word_characters_short():
    data = [
        ("jo&&se", "jose"),
        ("**li**", "li"),
        ("#::luisa", "luisa"),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["name", "expected_name"])\
        .withColumn("clean_name", remove_non_word_characters(F.col("name")))
    assert_column_equality(df, "clean_name", "expected_name")


def test_remove_non_word_characters_nice_error():
    data = [
        ("matt7", "matt"),
        ("bill&", "bill"),
        ("isabela*", "isabela"),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["name", "expected_name"])\
        .withColumn("clean_name", remove_non_word_characters(F.col("name")))
    with pytest.raises(ColumnsNotEqualError) as e_info:
        assert_column_equality(df, "clean_name", "expected_name")


def test_remove_non_word_characters_long():
    source_data = [
        ("jo&&se",),
        ("**li**",),
        ("#::luisa",),
        (None,)
    ]
    source_df = spark.createDataFrame(source_data, ["name"])

    actual_df = source_df.withColumn(
        "clean_name",
        remove_non_word_characters(F.col("name"))
    )

    expected_data = [
        ("jo&&se", "jose"),
        ("**li**", "li"),
        ("#::luisa", "luisa"),
        (None, None)
    ]
    expected_df = spark.createDataFrame(expected_data, ["name", "clean_name"])

    assert_df_equality(actual_df, expected_df)


def test_remove_non_word_characters_long_error():
    source_data = [
        ("matt7",),
        ("bill&",),
        ("isabela*",),
        (None,)
    ]
    source_df = spark.createDataFrame(source_data, ["name"])

    actual_df = source_df.withColumn(
        "clean_name",
        remove_non_word_characters(F.col("name"))
    )

    expected_data = [
        ("matt7", "matt"),
        ("bill&", "bill"),
        ("isabela*", "isabela"),
        (None, None)
    ]
    expected_df = spark.createDataFrame(expected_data, ["name", "clean_name"])

    with pytest.raises(DataFramesNotEqualError) as e_info:
        assert_df_equality(actual_df, expected_df)
