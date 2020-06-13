import pytest

from chispa import *
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


def test_approx_col_equality_same():
    data = [
        (1.1, 1.1),
        (2.2, 2.15),
        (3.3, 3.37),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["num1", "num2"])
    assert_approx_column_equality(df, "num1", "num2", 0.1)


def test_approx_col_equality_different():
    data = [
        (1.1, 1.1),
        (2.2, 2.15),
        (3.3, 5.0),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["num1", "num2"])
    with pytest.raises(ColumnsNotEqualError) as e_info:
        assert_approx_column_equality(df, "num1", "num2", 0.1)


def test_approx_df_equality_same():
    data1 = [
        (1.1, "a"),
        (2.2, "b"),
        (3.3, "c"),
        (None, None)
    ]
    df1 = spark.createDataFrame(data1, ["num", "letter"])

    data2 = [
        (1.05, "a"),
        (2.13, "b"),
        (3.3, "c"),
        (None, None)
    ]
    df2 = spark.createDataFrame(data2, ["num", "letter"])

    assert_approx_df_equality(df1, df2, 0.1)


def test_approx_df_equality_different():
    data1 = [
        (1.1, "a"),
        (2.2, "b"),
        (3.3, "c"),
        (None, None)
    ]
    df1 = spark.createDataFrame(data1, ["num", "letter"])

    data2 = [
        (1.1, "a"),
        (5.0, "b"),
        (3.3, "z"),
        (None, None)
    ]
    df2 = spark.createDataFrame(data2, ["num", "letter"])

    with pytest.raises(DataFramesNotEqualError) as e_info:
        assert_approx_df_equality(df1, df2, 0.1)


def test_schema_mismatch_message():
    data1 = [
        (1, "a"),
        (2, "b"),
        (3, "c"),
        (None, None)
    ]
    df1 = spark.createDataFrame(data1, ["num", "letter"])

    data2 = [
        (1, 6),
        (2, 7),
        (3, 8),
        (None, None)
    ]
    df2 = spark.createDataFrame(data2, ["num", "num2"])

    with pytest.raises(SchemasNotEqualError) as e_info:
        assert_df_equality(df1, df2)

