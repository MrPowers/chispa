import copy
from dataclasses import dataclass, field
from typing import List, Iterator, Any, Optional, Hashable

from faker import Factory, Generator
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField

# TODO add missing Spark types.
DEFAULT_CONFIG: dict = {
    "StringType": {"provider": "pystr"},
    "ByteType": {"provider": "pyint", "kwargs": {"min_value": -128, "max_value": 127}},
    "ShortType": {"provider": "pyint", "kwargs": {"min_value": -32768, "max_value": 32767}},
    "IntegerType": {"provider": "pyint", "kwargs": {"min_value": -2147483648, "max_value": 2147483647}},
    "LongType": {"provider": "pyint", "kwargs": {"min_value": -9223372036854775808, "max_value": 9223372036854775807}},
    "DoubleType": {"provider": "pyfloat"},
    "FloatType": {"provider": "pyfloat"},
    "DecimalType(10,0)": {"provider": "pydecimal", "kwargs": {"left_digits": 10, "right_digits": 0}},
    "DateType": {"provider": "date_object"},
    "TimestampType": {"provider": "date_time"},
    "BooleanType": {"provider": "pybool"},
    "BinaryType": {"provider": "binary", "kwargs": {"length": 64}}
}


class DataTypeMissingException(Exception):
    """DataType missing from default config"""

    def __init__(self):
        super().__init__("DataType missing from default config")


class PropertyCheckException(Exception):
    """Property Check failed"""

    def __init__(self, message):
        super().__init__(f"Property Check failed: {message}")


@dataclass
class Report:
    property_check: bool
    dataframe: DataFrame


@dataclass
class DataConfig:
    data_type: str
    provider: str
    kwargs: dict = field(default_factory=dict)


def default_field(obj):
    """
    Method to deal with complex objects when using default values for dataclasses.
    """
    return field(default_factory=lambda: copy.copy(obj))


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
    transformer: Any = None
    num_dataframes: int = 10
    default_config: dict = default_field(DEFAULT_CONFIG)
    config: dict = field(default_factory=dict)
    num_records: int = 10
    faker: Generator = Factory.create()
    seed: Optional[Hashable] = None

    def arbitrary_dataframes(self) -> Iterator[DataFrame]:
        """
        Method to generate a number of DataFrames (default is 10).
        :return: An iterator with DataFrames.
        """
        return map(
            lambda x: self.transformer(self.generate_data()) if self.transformer is not None else self.generate_data(),
            range(0, self.num_dataframes)
        )

    def get_datatype_and_provider(self, schema_field: StructField) -> DataConfig:
        """
        Helper function to get the needed datatype and Faker provider for a specific field.

        :param schema_field: The field from the schema.
        :return: The data type of the field and Faker provider.
        """
        if schema_field.name in self.config:
            data_type = str(self.config[schema_field.name].get("data_type"))
            provider = self.config[schema_field.name].get("provider")
            kwargs = self.config[schema_field.name].get("kwargs")
        else:
            data_type = str(schema_field.dataType)
            config: dict = self.default_config.get(data_type)
            if config is not None:
                provider = config.get("provider")
            else:
                raise DataTypeMissingException
            kwargs: dict = config.get("kwargs", {})
        return DataConfig(data_type=data_type, provider=provider, kwargs=kwargs)

    def use_providers(self, data_config: DataConfig):
        """
        Method to make use of the Faker providers to generate the data.

        :param data_config: DataConfig object.
        :return: Values from the Faker providers.
        """
        if self.seed is not None:
            self.faker.seed(seed=self.seed)
        return getattr(self.faker, data_config.provider)(**data_config.kwargs) if data_config.kwargs else getattr(
            self.faker, data_config.provider)()

    def generate_data(self) -> DataFrame:
        """
        Method that will generate fake data for for all fields and then put it into a DataFrame.
        A config can be pass with a specific provider for a certain column. This method does not use UDFs.

        :return: A DataFrame from the data created by Faker.
        """
        data_configs: List[DataConfig] = [
            self.get_datatype_and_provider(schema_field=schema_field) for schema_field in
            self.schema.fields
        ]
        data: Iterator = map(lambda x: list(map(self.use_providers, data_configs)), range(0, self.num_records))
        spark: SparkSession = SparkSession.builder.getOrCreate()
        return spark.createDataFrame(data=data, schema=self.schema)


def for_all(dfs: Iterator[DataFrame], property_to_check: Any) -> Iterator[Report]:
    """
    Method that will run a property over all the DataFrames in the iterator and returns an iterator
    with the boolean result.

    :param dfs: A generator with DataFrames.
    :param property_to_check: A function that has a DataFrame as a parameter and returns a bool.
    :return: An iterator of Report objects.
    """
    return map(lambda df: Report(property_check=property_to_check(df), dataframe=df), dfs)


def check_property(reports: Iterator[Report], function_to_run_in_failure: Optional[Any] = None) -> bool:
    """
    Method that will assert if the property that ran over the DataFrames is true for all of them.

    :param reports: The reports that contains the property result with the corresponding DataFrame.
    :param function_to_run_in_failure: Optional function to run over the failed DataFrames.
    :return: True if the property checked is true for all DataFrames, raises PropertyCheckException with the list
    of failed DataFrames.
    """
    l: List[Report] = list(reports)
    failed: List[DataFrame] = [elem.dataframe for elem in l if elem.property_check is False]
    if failed:
        if function_to_run_in_failure is not None:
            for df in failed:
                function_to_run_in_failure(df)
        raise PropertyCheckException(f"\n{[elem.collect() for elem in failed]}")
    else:
        return True
