from __future__ import annotations

from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StructField, StructType

from chispa.structfield_comparer import are_structfields_equal


def describe_are_structfields_equal():
    def it_returns_true_when_structfields_are_the_same():
        sf1 = StructField("hi", IntegerType(), True)
        sf2 = StructField("hi", IntegerType(), True)
        assert are_structfields_equal(sf1, sf2) is True

    def it_returns_false_when_column_names_are_different():
        sf1 = StructField("hello", IntegerType(), True)
        sf2 = StructField("hi", IntegerType(), True)
        assert are_structfields_equal(sf1, sf2) is False

    def it_returns_false_when_nullable_property_is_different():
        sf1 = StructField("hi", IntegerType(), False)
        sf2 = StructField("hi", IntegerType(), True)
        assert are_structfields_equal(sf1, sf2) is False

    def it_can_perform_nullability_insensitive_comparisons():
        sf1 = StructField("hi", IntegerType(), False)
        sf2 = StructField("hi", IntegerType(), True)
        assert are_structfields_equal(sf1, sf2, ignore_nullability=True) is True

    def it_returns_true_when_nested_types_are_the_same():
        sf1 = StructField("hi", StructType([StructField("world", IntegerType(), False)]), False)
        sf2 = StructField("hi", StructType([StructField("world", IntegerType(), False)]), False)
        assert are_structfields_equal(sf1, sf2) is True

    def it_returns_false_when_nested_names_are_different():
        sf1 = StructField("hi", StructType([StructField("world", IntegerType(), False)]), False)
        sf2 = StructField("hi", StructType([StructField("developer", IntegerType(), False)]), False)
        assert are_structfields_equal(sf1, sf2) is False

    def it_returns_false_when_nested_types_are_different():
        sf1 = StructField("hi", StructType([StructField("world", IntegerType(), False)]), False)
        sf2 = StructField("hi", StructType([StructField("world", DoubleType(), False)]), False)
        assert are_structfields_equal(sf1, sf2) is False

    def it_returns_false_when_nested_types_have_different_nullability():
        sf1 = StructField("hi", StructType([StructField("world", IntegerType(), False)]), False)
        sf2 = StructField("hi", StructType([StructField("world", IntegerType(), True)]), False)
        assert are_structfields_equal(sf1, sf2) is False

    def it_returns_false_when_nested_types_are_different_with_ignore_nullable_true():
        sf1 = StructField("hi", StructType([StructField("world", IntegerType(), False)]), False)
        sf2 = StructField("hi", StructType([StructField("developer", IntegerType(), False)]), False)
        assert are_structfields_equal(sf1, sf2, ignore_nullability=True) is False

    def it_returns_true_when_nested_types_have_different_nullability_with_ignore_null_true():
        sf1 = StructField("hi", StructType([StructField("world", IntegerType(), False)]), False)
        sf2 = StructField("hi", StructType([StructField("world", IntegerType(), True)]), False)
        assert are_structfields_equal(sf1, sf2, ignore_nullability=True) is True

    def it_returns_true_when_inner_metadata_is_different_but_ignored():
        sf1 = StructField("hi", StructType([StructField("world", IntegerType(), False)]), False)
        sf2 = StructField("hi", StructType([StructField("world", IntegerType(), False, {"a": "b"})]), False)
        assert are_structfields_equal(sf1, sf2, ignore_metadata=True) is True

    def it_returns_true_when_inner_array_metadata_is_different_but_ignored():
        sf1 = StructField(
            "hi",
            ArrayType(
                StructType([
                    StructField("world", IntegerType(), True, {"comment": "Comment"}),
                ]),
                True,
            ),
            True,
        )
        sf2 = StructField(
            "hi",
            ArrayType(
                StructType([
                    StructField("world", IntegerType(), True, {"comment": "Some other comment"}),
                ]),
                True,
            ),
            True,
        )
        assert are_structfields_equal(sf1, sf2, ignore_metadata=True) is True
