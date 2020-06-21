import pytest

from pyspark.sql.types import *
from chispa.schema_comparer import *


def test_assert_schema_equality_equal():
    s1 = StructType([
       StructField("name", StringType(), True),
       StructField("age", IntegerType(), True)])
    s2 = StructType([
       StructField("name", StringType(), True),
       StructField("age", IntegerType(), True)])
    assert_schema_equality(s1, s2)


def test_assert_schema_equality_error():
    s1 = StructType([
       StructField("HAHA", StringType(), True),
       StructField("age", IntegerType(), True)])
    s2 = StructType([
       StructField("name", StringType(), True),
       StructField("age", IntegerType(), True)])
    with pytest.raises(SchemasNotEqualError) as e_info:
        assert_schema_equality(s1, s2)


def test_assert_schema_equality_length_error():
    s1 = StructType([
       StructField("name", StringType(), True),
       StructField("age", IntegerType(), True)])
    s2 = StructType([
       StructField("name", StringType(), True),
       StructField("age", IntegerType(), True),
       StructField("fav_number", IntegerType(), True)])
    with pytest.raises(SchemasNotEqualError) as e_info:
        assert_schema_equality(s1, s2)


def test_assert_schema_equality_ignore_nullable_completely_equal():
    s1 = StructType([
       StructField("name", StringType(), True),
       StructField("age", IntegerType(), True)])
    s2 = StructType([
       StructField("name", StringType(), True),
       StructField("age", IntegerType(), True)])
    assert_schema_equality_ignore_nullable(s1, s2)


def test_assert_schema_equality_ignore_nullable_basically_equal():
    s1 = StructType([
       StructField("name", StringType(), True),
       StructField("age", IntegerType(), True)])
    s2 = StructType([
       StructField("name", StringType(), True),
       StructField("age", IntegerType(), False)])
    assert_schema_equality_ignore_nullable(s1, s2)


def test_are_schemas_equal_ignore_nullable_equal():
    s1 = StructType([
       StructField("name", StringType(), True),
       StructField("age", IntegerType(), True)])
    s2 = StructType([
       StructField("name", StringType(), True),
       StructField("age", IntegerType(), False)])
    assert are_schemas_equal_ignore_nullable(s1, s2) == True


def test_are_schemas_equal_ignore_nullable_unequal():
    s1 = StructType([
       StructField("blah", StringType(), True),
       StructField("age", IntegerType(), True)])
    s2 = StructType([
       StructField("name", StringType(), True),
       StructField("age", IntegerType(), False)])
    assert are_schemas_equal_ignore_nullable(s1, s2) == False
