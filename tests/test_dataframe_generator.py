from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructType, StructField, LongType, DateType

from chispa.dataframe_generator import DataFrameGenerator, for_all, check_property

SCHEMA = StructType([
    StructField("num", LongType(), True),
    StructField("expected_name", StringType(), True),
    StructField("date", DateType(), True)
])


def describe_dataframe_generator():
    def test_generate_in_spark():
        df_gen = DataFrameGenerator(schema=SCHEMA)
        df_gen.generate_in_spark().show(truncate=False)

    def test_generate_in_spark_one_million_records():
        df_gen = DataFrameGenerator(schema=SCHEMA, num_records=1000000)
        print(df_gen.generate_in_spark().count())

    def test_generate_in_spark_with_config():
        config = {"bank_account": {
            "data_type": StringType(),
            "provider": "iban"
        }}
        schema = StructType([
            StructField("bank_account", StringType(), True)
        ])
        df_gen = DataFrameGenerator(schema=schema, config=config)
        df_gen.generate_in_spark().show(truncate=False)

    def test_generate_locally():
        df_gen = DataFrameGenerator(schema=SCHEMA)
        df_gen.generate_locally().show(truncate=False)

    def test_generate_locally_one_million_records():
        df_gen = DataFrameGenerator(schema=SCHEMA, num_records=1000000)
        print(df_gen.generate_in_spark().count())

    def test_generate_locally_with_config():
        config = {"bank_account": {
            "data_type": StringType(),
            "provider": "iban"
        }}
        schema = StructType([
            StructField("bank_account", StringType(), True)
        ])
        df_gen = DataFrameGenerator(schema=schema, config=config)
        df_gen.generate_locally().show(truncate=False)

    def test_stuff():
        df_gen = DataFrameGenerator(schema=SCHEMA, num_dataframes=2)
        arbitrary_dfs = df_gen.arbitrary_dataframe_spark()
        x = for_all(arbitrary_dfs, lambda df: df.schema == SCHEMA and df.count() >= 100)
        check_property(x)

