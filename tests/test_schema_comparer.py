import pytest

from pyspark.sql.types import *
from chispa.schema_comparer import *


def describe_assert_schema_equality():
    def it_does_nothing_when_equal():
        s1 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), True)])
        s2 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), True)])
        assert_schema_equality(s1, s2)


    def it_throws_when_column_names_differ():
        s1 = StructType([
           StructField("HAHA", StringType(), True),
           StructField("age", IntegerType(), True)])
        s2 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), True)])
        with pytest.raises(SchemasNotEqualError) as e_info:
            assert_schema_equality(s1, s2)


    def it_throws_when_schema_lengths_differ():
        s1 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), True)])
        s2 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), True),
           StructField("fav_number", IntegerType(), True)])
        with pytest.raises(SchemasNotEqualError) as e_info:
            assert_schema_equality(s1, s2)


def describe_assert_schema_equality_ignore_nullable():
    def it_does_nothing_when_equal():
        s1 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), True)])
        s2 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), True)])
        assert_schema_equality_ignore_nullable(s1, s2)


    def it_does_nothing_when_only_nullable_flag_is_different():
        s1 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), True)])
        s2 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), False)])
        assert_schema_equality_ignore_nullable(s1, s2)


def describe_are_schemas_equal_ignore_nullable():
    def it_returns_true_when_only_nullable_flag_is_different():
        s1 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), True)])
        s2 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), False)])
        assert are_schemas_equal_ignore_nullable(s1, s2) == True


    def it_returns_false_when_column_names_differ():
        s1 = StructType([
           StructField("blah", StringType(), True),
           StructField("age", IntegerType(), True)])
        s2 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), False)])
        assert are_schemas_equal_ignore_nullable(s1, s2) == False
