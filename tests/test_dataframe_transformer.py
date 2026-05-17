from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, IntegerType, LongType, MapType, StringType, StructField, StructType

from chispa.dataframe_transformer import flatten_dataframe


def describe_flatten_dataframe():
    def it_flattens_struct_fields(spark: SparkSession):
        data = [
            {"id": 1, "name": "Cole Volk", "fitness": {"height": 130, "weight": 60}},
            {"name": "Mark Reg", "fitness": {"height": 130, "weight": 60}},
            {"id": 2, "name": "Faye Raker", "fitness": {"height": 130, "weight": 60}},
        ]
        df = spark.createDataFrame(data)

        flat_df = flatten_dataframe(df, sep=":")

        assert "fitness:height" in flat_df.columns
        assert "fitness:weight" in flat_df.columns
        assert "fitness" not in flat_df.columns

    def it_flattens_map_fields(spark: SparkSession):
        data = [
            {"state": "Florida", "shortname": "FL", "info": {"governor": "Rick Scott"}},
            {"state": "Ohio", "shortname": "OH", "info": {"governor": "John Kasich"}},
        ]
        df = spark.createDataFrame(data)

        flat_df = flatten_dataframe(df, sep=":")

        assert "info:governor" in flat_df.columns
        assert "info" not in flat_df.columns
        assert "state" in flat_df.columns
        assert "shortname" in flat_df.columns

    def it_flattens_array_fields(spark: SparkSession):
        data = [
            {"name": "John", "scores": [85, 90, 95]},
            {"name": "Jane", "scores": [88, 92, 94]},
        ]
        df = spark.createDataFrame(data)

        flat_df = flatten_dataframe(df)

        # Arrays are exploded, so the original column is removed
        assert "scores" not in flat_df.columns

    def it_flattens_mixed_complex_types(spark: SparkSession):
        data_mixed = [
            {
                "state": "Florida",
                "shortname": "FL",
                "info": {"governor": "Rick Scott"},
                "counties": [
                    {"name": "Dade", "population": 12345},
                    {"name": "Broward", "population": 40000},
                    {"name": "Palm Beach", "population": 60000},
                ],
            },
            {
                "state": "Ohio",
                "shortname": "OH",
                "info": {"governor": "John Kasich"},
                "counties": [
                    {"name": "Summit", "population": 1234},
                    {"name": "Cuyahoga", "population": 1337},
                ],
            },
        ]
        df = spark.createDataFrame(data_mixed)

        flat_df = flatten_dataframe(df, sep=":")

        # Check that complex fields are flattened
        assert "info:governor" in flat_df.columns
        assert "info" not in flat_df.columns
        # Arrays are exploded
        assert "counties" not in flat_df.columns
        # Simple fields remain
        assert "state" in flat_df.columns
        assert "shortname" in flat_df.columns

    def it_uses_default_separator(spark: SparkSession):
        data = [{"id": 1, "name": "Cole", "fitness": {"height": 130, "weight": 60}}]
        df = spark.createDataFrame(data)

        flat_df = flatten_dataframe(df)

        assert "fitness_height" in flat_df.columns
        assert "fitness_weight" in flat_df.columns

    def it_uses_custom_separator(spark: SparkSession):
        data = [{"id": 1, "name": "Cole", "fitness": {"height": 130, "weight": 60}}]
        df = spark.createDataFrame(data)

        flat_df = flatten_dataframe(df, sep=":")

        assert "fitness:height" in flat_df.columns
        assert "fitness:weight" in flat_df.columns

    def it_preserves_simple_fields(spark: SparkSession):
        data = [
            {"id": 1, "name": "Cole", "age": 25},
            {"id": 2, "name": "Jane", "age": 30},
        ]
        df = spark.createDataFrame(data)

        flat_df = flatten_dataframe(df)

        assert set(flat_df.columns) == {"id", "name", "age"}

    def it_handles_nested_structs(spark: SparkSession):
        data = [
            ((("James", None, "Smith"),), "OH", "M"),
            (("Anna", "Rose", ""), "NY", "F"),
        ]
        schema = StructType(
            [
                StructField(
                    "name",
                    StructType(
                        [
                            StructField("firstname", StringType(), True),
                            StructField("middlename", StringType(), True),
                            StructField("lastname", StringType(), True),
                        ]
                    ),
                ),
                StructField("state", StringType(), True),
                StructField("gender", StringType(), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        flat_df = flatten_dataframe(df, sep=":")

        assert "name:firstname" in flat_df.columns
        assert "name:middlename" in flat_df.columns
        assert "name:lastname" in flat_df.columns
        assert "name" not in flat_df.columns
        assert "state" in flat_df.columns
        assert "gender" in flat_df.columns

    def it_handles_empty_dataframe(spark: SparkSession):
        data = []
        df = spark.createDataFrame(data, "id INT, name STRING")

        flat_df = flatten_dataframe(df)

        assert set(flat_df.columns) == {"id", "name"}

    def it_handles_dataframe_with_only_simple_fields(spark: SparkSession):
        data = [{"id": 1, "name": "Cole"}, {"id": 2, "name": "Jane"}]
        df = spark.createDataFrame(data)

        flat_df = flatten_dataframe(df)

        assert set(flat_df.columns) == {"id", "name"}

    def it_handles_map_with_multiple_keys(spark: SparkSession):
        data = [
            {"state": "FL", "info": {"governor": "Rick Scott", "capital": "Tallahassee"}},
            {"state": "OH", "info": {"governor": "John Kasich", "capital": "Columbus"}},
        ]
        df = spark.createDataFrame(data)

        flat_df = flatten_dataframe(df, sep=":")

        assert "info:governor" in flat_df.columns
        assert "info:capital" in flat_df.columns
        assert "info" not in flat_df.columns

    def it_handles_struct_with_multiple_fields(spark: SparkSession):
        data = [
            {
                "person": {"first": "John", "middle": "Q", "last": "Doe"},
                "age": 30,
            },
            {
                "person": {"first": "Jane", "middle": "A", "last": "Smith"},
                "age": 25,
            },
        ]
        df = spark.createDataFrame(data)

        flat_df = flatten_dataframe(df, sep="_")

        assert "person_first" in flat_df.columns
        assert "person_middle" in flat_df.columns
        assert "person_last" in flat_df.columns
        assert "person" not in flat_df.columns
        assert "age" in flat_df.columns
