from pyspark.sql.types import *

from chispa.schema_comparer import are_structfields_equal_ignore_nullable

def describe_are_structfields_equal_ignore_nullable():
    def it_returns_true_when_structfields_are_the_same():
        sf1 = StructField("hi", IntegerType(), True)
        sf2 = StructField("hi", IntegerType(), True)
        assert are_structfields_equal_ignore_nullable(sf1, sf2) == True

    def it_returns_false_when_column_names_are_different():
        sf1 = StructField("hello", IntegerType(), True)
        sf2 = StructField("hi", IntegerType(), True)
        assert are_structfields_equal_ignore_nullable(sf1, sf2) == False

    def it_can_perform_nullability_insensitive_comparisons():
        sf1 = StructField("hi", IntegerType(), False)
        sf2 = StructField("hi", IntegerType(), True)
        assert are_structfields_equal_ignore_nullable(sf1, sf2) == True

    def it_returns_true_when_nested_types_are_the_same():
        sf1 = StructField("hi", StructType([StructField("world", IntegerType(), False)]), False)
        sf2 = StructField("hi", StructType([StructField("world", IntegerType(), False)]), False)
        assert are_structfields_equal_ignore_nullable(sf1, sf2) == True

    def it_returns_false_when_nested_names_are_different():
        sf1 = StructField("hi", StructType([StructField("world", IntegerType(), False)]), False)
        sf2 = StructField("hi", StructType([StructField("developer", IntegerType(), False)]), False)
        assert are_structfields_equal_ignore_nullable(sf1, sf2) == False

    def it_returns_false_when_nested_types_are_different():
        sf1 = StructField("hi", StructType([StructField("world", IntegerType(), False)]), False)
        sf2 = StructField("hi", StructType([StructField("world", DoubleType(), False)]), False)
        assert are_structfields_equal_ignore_nullable(sf1, sf2) == False

    def it_returns_false_when_nested_types_are_different_with_ignore_nullable_true():
        sf1 = StructField("hi", StructType([StructField("world", IntegerType(), False)]), False)
        sf2 = StructField("hi", StructType([StructField("developer", IntegerType(), False)]), False)
        assert are_structfields_equal_ignore_nullable(sf1, sf2) == False

    def it_returns_true_when_nested_types_have_different_nullability_with_ignore_null_true():
        sf1 = StructField("hi", StructType([StructField("world", IntegerType(), False)]), False)
        sf2 = StructField("hi", StructType([StructField("world", IntegerType(), True)]), False)
        assert are_structfields_equal_ignore_nullable(sf1, sf2) == True