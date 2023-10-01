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
    def it_has_good_error_messages_for_different_sized_schemas():
        s1 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), True)])
        s2 = StructType([
           StructField("name", StringType(), False),
           StructField("age", IntegerType(), True),
           StructField("something", IntegerType(), True),
           StructField("else", IntegerType(), True)
        ])
        with pytest.raises(SchemasNotEqualError) as e_info:
            assert_schema_equality_ignore_nullable(s1, s2)


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
           StructField("age", IntegerType(), True),
           StructField("coords", ArrayType(DoubleType(), True), True),
        ])
        s2 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), False),
           StructField("coords", ArrayType(DoubleType(), True), False),
        ])
        assert are_schemas_equal_ignore_nullable(s1, s2) == True


    def it_returns_true_when_only_nullable_flag_is_different_within_array_element():
        s1 = StructType([StructField("coords", ArrayType(DoubleType(), True), True)])
        s2 = StructType([StructField("coords", ArrayType(DoubleType(), False), True)])
        assert are_schemas_equal_ignore_nullable(s1, s2) == True

    def it_returns_true_when_only_nullable_flag_is_different_within_nested_array_element():
        s1 = StructType([StructField("coords", ArrayType(ArrayType(DoubleType(), True), True), True)])
        s2 = StructType([StructField("coords", ArrayType(ArrayType(DoubleType(), False), True), True)])
        assert are_schemas_equal_ignore_nullable(s1, s2) == True


    def it_returns_false_when_the_element_type_is_different_within_array():
        s1 = StructType([StructField("coords", ArrayType(DoubleType(), True), True)])
        s2 = StructType([StructField("coords", ArrayType(IntegerType(), True), True)])
        assert are_schemas_equal_ignore_nullable(s1, s2) == False


    def it_returns_false_when_column_names_differ():
        s1 = StructType([
           StructField("blah", StringType(), True),
           StructField("age", IntegerType(), True)])
        s2 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), False)])
        assert are_schemas_equal_ignore_nullable(s1, s2) == False

    def it_returns_false_when_columns_have_different_order():
        s1 = StructType([
           StructField("blah", StringType(), True),
           StructField("age", IntegerType(), True)])
        s2 = StructType([
           StructField("age", IntegerType(), False),
           StructField("blah", StringType(), True)])
        assert are_schemas_equal_ignore_nullable(s1, s2) == False


def describe_are_structfields_equal():
    def it_returns_true_when_only_nullable_flag_is_different_within_array_element():
        s1 = StructField("coords", ArrayType(DoubleType(), True), True)
        s2 = StructField("coords", ArrayType(DoubleType(), False), True)
        assert are_structfields_equal(s1, s2, True) == True


    def it_returns_false_when_the_element_type_is_different_within_array():
        s1 = StructField("coords", ArrayType(DoubleType(), True), True)
        s2 = StructField("coords", ArrayType(IntegerType(), True), True)
        assert are_structfields_equal(s1, s2, True) == False


    def it_returns_true_when_the_element_type_is_same_within_struct():
        s1 = StructField("coords", StructType([StructField("hello", DoubleType(), True)]), True)
        s2 = StructField("coords", StructType([StructField("hello", DoubleType(), True)]), True)
        assert are_structfields_equal(s1, s2, True) == True


    def it_returns_false_when_the_element_type_is_different_within_struct():
        s1 = StructField("coords", StructType([StructField("hello", DoubleType(), True)]), True)
        s2 = StructField("coords", StructType([StructField("hello", IntegerType(), True)]), True)
        assert are_structfields_equal(s1, s2, True) == False


    def it_returns_false_when_the_element_name_is_different_within_struct():
        s1 = StructField("coords", StructType([StructField("hello", DoubleType(), True)]), True)
        s2 = StructField("coords", StructType([StructField("world", DoubleType(), True)]), True)
        assert are_structfields_equal(s1, s2, True) == False
       
        
    def it_returns_true_when_different_nullability_within_struct():
        s1 = StructField("coords", StructType([StructField("hello", DoubleType(), True)]), True)
        s2 = StructField("coords", StructType([StructField("hello", DoubleType(), False)]), True)
        assert are_structfields_equal(s1, s2, True) == True
    def it_returns_false_when_metadata_differs():
        s1 = StructField("coords", StringType(), True, {"hi": "whatever"})
        s2 = StructField("coords", StringType(), True, {"hi": "no"})
        assert are_structfields_equal(s1, s2, ignore_nullability=True, ignore_metadata=False) is False

    def it_allows_metadata_to_be_ignored():
        s1 = StructField("coords", StringType(), True, {"hi": "whatever"})
        s2 = StructField("coords", StringType(), True, {"hi": "no"})
        assert are_structfields_equal(s1, s2, ignore_nullability=False, ignore_metadata=True) is True

    def it_allows_nullability_and_metadata_to_be_ignored():
        s1 = StructField("coords", StringType(), True, {"hi": "whatever"})
        s2 = StructField("coords", StringType(), False, {"hi": "no"})
        assert are_structfields_equal(s1, s2, ignore_nullability=True, ignore_metadata=True) is True

    def it_returns_true_when_metadata_is_the_same():
        s1 = StructField("coords", StringType(), True, {"hi": "whatever"})
        s2 = StructField("coords", StringType(), True, {"hi": "whatever"})
        assert are_structfields_equal(s1, s2, ignore_nullability=True, ignore_metadata=False) is True
