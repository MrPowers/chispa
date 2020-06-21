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
