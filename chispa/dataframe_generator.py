import copy
import uuid
from dataclasses import dataclass, field
from functools import reduce
from typing import List, Dict, Iterator, Any

import cloudpickle
import pyspark.serializers
from faker import Factory, Generator
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField

pyspark.serializers.cloudpickle = cloudpickle


def default_field(obj):
    """
    Method to deal with complex objects when using default values for dataclasses.
    """
    return field(default_factory=lambda: copy.copy(obj))


def for_all(dfs: Iterator[DataFrame], property_to_check: Any) -> Iterator[bool]:
    """
    Method that will run a property over all the DataFrames in the iterator and returns an iterator
    with the boolean result.

    :param dfs: A generator with DataFrames.
    :param property_to_check: A function that has a DataFrame as a parameter and returns a bool.
    :return:
    """
    return map(lambda df: property_to_check(df), dfs)


def check_property(reports: Iterator[bool]):
    """
    Method that will assert if the property that ran over the DataFrames is true for all of them.

    :param reports:
    :return:
    """
    assert all(list(reports)) is True


@dataclass
class DataFrameGenerator:
    """
    Dataclass that has all the methods and variables necessary to generate fake data.
    schema should be a StructType object:
     StructType([
         StructField("id", LongType(), True),
         StructField("name", StringType(), True)
     ])
    And config is a dictionary with the column name as the key and then another dictionary with the type
    and provider:
      config = {"bank_account": {
         "data_type": StringType(),
         "provider": "iban"
        }}
    """
    schema: StructType
    num_dataframes: int = 10
    config: dict = field(default_factory=dict)
    id_column_name = str(uuid.uuid4()).replace("-", "_")
    num_records: int = 100
    faker: Generator = Factory.create()
    faker_spark_types: Dict[str, str] = default_field({
        "StringType": "text",
        "IntegerType": "random_int",
        "LongType": "random_int",
        "DateType": "date_object"
    })

    # TODO the data in the DataFrame needs to be randomized more, perhaps check for Faker return types and use that
    # to randomize the data in the columns a bit more.
    def arbitrary_dataframe_spark(self) -> Iterator[DataFrame]:
        """
        Method to generate a number of DataFrames (default is 10).
        :return: An iterator with DataFrames.
        """
        return map(lambda x: self.generate_in_spark(), range(0, self.num_dataframes))

    def generate_initial_dataframe(self) -> DataFrame:
        """
        Method to generate the initial DataFrame consisting of one column with a certain number of records.
        """
        data = [(elem,) for elem in range(0, self.num_records)]
        spark: SparkSession = SparkSession.builder.getOrCreate()
        return spark.createDataFrame(data, [self.id_column_name])

    def generate_in_spark(self) -> DataFrame:
        """
        Method that will take the Spark DataFrame schema provided to the dataclass,
        iterate over it's fields and call the generate_fake_data method.
        """
        return reduce(
            lambda dataframe, schema_field:
            self.generate_fake_data(dataframe=dataframe, schema_field=schema_field), self.schema,
            self.generate_initial_dataframe()
        ).drop(self.id_column_name)

    def generate_fake_data(self, dataframe: DataFrame, schema_field: StructField) -> DataFrame:
        """
        Method that will generate fake data for a specific field and add it to the DataFrame provided using UDFs.
        A config can be pass with a specific provider for a certain column.

        :param dataframe: The DataFrame to add data to.
        :param schema_field: The field from the schema to add to the DataFrame.
        :return: The DataFrame with the column added.
        """
        data_type, provider = self.get_datatype_and_provider(schema_field)
        function = getattr(self.faker, provider)
        fake_factory_udf = udf(function, data_type)
        return dataframe.withColumn(schema_field.name, fake_factory_udf())

    def get_datatype_and_provider(self, schema_field: StructField):
        """
        Helper function to get the needed datatype and Faker provider for a specific field.

        :param schema_field: The field from the schema.
        :return: The data type of the field and Faker provider.
        """
        if schema_field.name in self.config:
            data_type = self.config[schema_field.name].get("data_type")
            provider = self.config[schema_field.name].get("provider")
        else:
            data_type = schema_field.dataType
            provider = self.faker_spark_types.get(str(schema_field.dataType))
        return data_type, provider

    def generate_locally(self) -> DataFrame:
        """
        Method that will generate fake data for for all fields and then put it into a DataFrame.
        A config can be pass with a specific provider for a certain column. This method does not use UDFs.
        """
        providers: List[str] = [self.get_datatype_and_provider(schema_field)[1] for schema_field in self.schema.fields]
        data = map(
            lambda x: list(map(
                lambda provider:
                getattr(self.faker, provider)(),
                providers)),
            range(1, self.num_records + 1))
        spark: SparkSession = SparkSession.builder.getOrCreate()
        return spark.createDataFrame(data, self.schema.names)
