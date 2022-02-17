# chispa

![CI](https://github.com/MrPowers/chispa/workflows/CI/badge.svg?branch=master)

chispa provides fast PySpark test helper methods that output descriptive error messages.

This library makes it easy to write high quality PySpark code.

Fun fact: "chispa" means Spark in Spanish ;)

## Installation

Install the latest version with `pip install chispa`.

If you use Poetry, add this library as a development dependency with `poetry add chispa --dev`.

## Column equality

Suppose you have a function that removes the non-word characters in a string.

```python
def remove_non_word_characters(col):
    return F.regexp_replace(col, "[^\\w\\s]+", "")
```

Create a `SparkSession` so you can create DataFrames.

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
  .master("local")
  .appName("chispa")
  .getOrCreate())
```

Create a DataFrame with a column that contains strings with non-word characters, run the `remove_non_word_characters` function, and check that all these characters are removed with the chispa `assert_column_equality` method.

```python
import pytest

from chispa.column_comparer import assert_column_equality
import pyspark.sql.functions as F

def test_remove_non_word_characters_short():
    data = [
        ("jo&&se", "jose"),
        ("**li**", "li"),
        ("#::luisa", "luisa"),
        (None, None)
    ]
    df = (spark.createDataFrame(data, ["name", "expected_name"])
        .withColumn("clean_name", remove_non_word_characters(F.col("name"))))
    assert_column_equality(df, "clean_name", "expected_name")
```

Let's write another test that'll fail to see how the descriptive error message lets you easily debug the underlying issue.

Here's the failing test:

```python
def test_remove_non_word_characters_nice_error():
    data = [
        ("matt7", "matt"),
        ("bill&", "bill"),
        ("isabela*", "isabela"),
        (None, None)
    ]
    df = (spark.createDataFrame(data, ["name", "expected_name"])
        .withColumn("clean_name", remove_non_word_characters(F.col("name"))))
    assert_column_equality(df, "clean_name", "expected_name")
```

Here's the nicely formatted error message:

![ColumnsNotEqualError](https://github.com/MrPowers/chispa/blob/main/images/columns_not_equal_error.png)

You can see the `matt7` / `matt` row of data is what's causing the error (note it's highlighted in red).  The other rows are colored blue because they're equal.

## DataFrame equality

We can also test the `remove_non_word_characters` method by creating two DataFrames and verifying that they're equal.

Creating two DataFrames is slower and requires more code, but comparing entire DataFrames is necessary for some tests.

```python
from chispa.dataframe_comparer import *

def test_remove_non_word_characters_long():
    source_data = [
        ("jo&&se",),
        ("**li**",),
        ("#::luisa",),
        (None,)
    ]
    source_df = spark.createDataFrame(source_data, ["name"])

    actual_df = source_df.withColumn(
        "clean_name",
        remove_non_word_characters(F.col("name"))
    )

    expected_data = [
        ("jo&&se", "jose"),
        ("**li**", "li"),
        ("#::luisa", "luisa"),
        (None, None)
    ]
    expected_df = spark.createDataFrame(expected_data, ["name", "clean_name"])

    assert_df_equality(actual_df, expected_df)
```

Let's write another test that'll return an error, so you can see the descriptive error message.

```python
def test_remove_non_word_characters_long_error():
    source_data = [
        ("matt7",),
        ("bill&",),
        ("isabela*",),
        (None,)
    ]
    source_df = spark.createDataFrame(source_data, ["name"])

    actual_df = source_df.withColumn(
        "clean_name",
        remove_non_word_characters(F.col("name"))
    )

    expected_data = [
        ("matt7", "matt"),
        ("bill&", "bill"),
        ("isabela*", "isabela"),
        (None, None)
    ]
    expected_df = spark.createDataFrame(expected_data, ["name", "clean_name"])

    assert_df_equality(actual_df, expected_df)
```

Here's the nicely formatted error message:

![DataFramesNotEqualError](https://github.com/MrPowers/chispa/blob/main/images/dfs_not_equal_error.png)

### Ignore row order

You can easily compare DataFrames, ignoring the order of the rows.  The content of the DataFrames is usually what matters, not the order of the rows.

Here are the contents of `df1`:

```
+--------+
|some_num|
+--------+
|       1|
|       2|
|       3|
+--------+
```

Here are the contents of `df2`:

```
+--------+
|some_num|
+--------+
|       2|
|       1|
|       3|
+--------+
```

Here's how to confirm `df1` and `df2` are equal when the row order is ignored.

```python
assert_df_equality(df1, df2, ignore_row_order=True)
```

If you don't specify to `ignore_row_order` then the test will error out with this message:

![ignore_row_order_false](https://github.com/MrPowers/chispa/blob/main/images/ignore_row_order_false.png)

The rows aren't ordered by default because sorting slows down the function.

### Ignore column order

This section explains how to compare DataFrames, ignoring the order of the columns.

Suppose you have the following `df1`:

```
+----+----+
|num1|num2|
+----+----+
|   1|   7|
|   2|   8|
|   3|   9|
+----+----+
```

Here are the contents of `df2`:

```
+----+----+
|num2|num1|
+----+----+
|   7|   1|
|   8|   2|
|   9|   3|
+----+----+
```

Here's how to compare the equality of `df1` and `df2`, ignoring the column order:

```python
assert_df_equality(df1, df2, ignore_column_order=True)
```

Here's the error message you'll see if you run `assert_df_equality(df1, df2)`, without ignoring the column order.

![ignore_column_order_false](https://github.com/MrPowers/chispa/blob/main/images/ignore_column_order_false.png)

### Ignore nullability

Each column in a schema has three properties: a name, data type, and nullable property.  The column can accept null values if `nullable` is set to true.

You'll sometimes want to ignore the nullable property when making DataFrame comparisons.

Suppose you have the following `df1`:

```
+-----+---+
| name|age|
+-----+---+
| juan|  7|
|bruna|  8|
+-----+---+
```

And this `df2`:

```
+-----+---+
| name|age|
+-----+---+
| juan|  7|
|bruna|  8|
+-----+---+
```

You might be surprised to find that in this example, `df1` and `df2` are not equal and will error out with this message:

![nullable_off_error](https://github.com/MrPowers/chispa/blob/main/images/nullable_off_error.png)

Examine the code in this contrived example to better understand the error:

```python
def ignore_nullable_property():
    s1 = StructType([
       StructField("name", StringType(), True),
       StructField("age", IntegerType(), True)])
    df1 = spark.createDataFrame([("juan", 7), ("bruna", 8)], s1)
    s2 = StructType([
       StructField("name", StringType(), True),
       StructField("age", IntegerType(), False)])
    df2 = spark.createDataFrame([("juan", 7), ("bruna", 8)], s2)
    assert_df_equality(df1, df2)
```

You can ignore the nullable property when assessing equality by adding a flag:

```python
assert_df_equality(df1, df2, ignore_nullable=True)
```

Elements contained within an `ArrayType()` also have a nullable property, in addition to the nullable property of the column schema. These are also ignored when passing `ignore_nullable=True`.

Again, examine the following code to understand the error that `ignore_nullable=True` bypasses:

```python
def ignore_nullable_property_array():
    s1 = StructType([
        StructField("name", StringType(), True),
        StructField("coords", ArrayType(DoubleType(), True), True),])
    df1 = spark.createDataFrame([("juan", [1.42, 3.5]), ("bruna", [2.76, 3.2])], s1)
    s2 = StructType([
        StructField("name", StringType(), True),
        StructField("coords", ArrayType(DoubleType(), False), True),])
    df2 = spark.createDataFrame([("juan", [1.42, 3.5]), ("bruna", [2.76, 3.2])], s2)
    assert_df_equality(df1, df2)
```

### Allow NaN equality

Python has NaN (not a number) values and two NaN values are not considered equal by default.  Create two NaN values, compare them, and confirm they're not considered equal by default.

```python
nan1 = float('nan')
nan2 = float('nan')
nan1 == nan2 # False
```

Pandas, a popular DataFrame library, does consider NaN values to be equal by default.

This library requires you to set a flag to consider two NaN values to be equal.

```python
assert_df_equality(df1, df2, allow_nan_equality=True)
```

## Approximate column equality

We can check if columns are approximately equal, which is especially useful for floating number comparisons.

Here's a test that creates a DataFrame with two floating point columns and verifies that the columns are approximately equal.  In this example, values are considered approximately equal if the difference is less than 0.1.

```python
def test_approx_col_equality_same():
    data = [
        (1.1, 1.1),
        (2.2, 2.15),
        (3.3, 3.37),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["num1", "num2"])
    assert_approx_column_equality(df, "num1", "num2", 0.1)
```

Here's an example of a test with columns that are not approximately equal.

```python
def test_approx_col_equality_different():
    data = [
        (1.1, 1.1),
        (2.2, 2.15),
        (3.3, 5.0),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["num1", "num2"])
    assert_approx_column_equality(df, "num1", "num2", 0.1)
```

This failing test will output a readable error message so the issue is easy to debug.

![ColumnsNotEqualError](https://github.com/MrPowers/chispa/blob/main/images/columns_not_approx_equal.png)

## Approximate DataFrame equality

Let's create two DataFrames and confirm they're approximately equal.

```python
def test_approx_df_equality_same():
    data1 = [
        (1.1, "a"),
        (2.2, "b"),
        (3.3, "c"),
        (None, None)
    ]
    df1 = spark.createDataFrame(data1, ["num", "letter"])

    data2 = [
        (1.05, "a"),
        (2.13, "b"),
        (3.3, "c"),
        (None, None)
    ]
    df2 = spark.createDataFrame(data2, ["num", "letter"])

    assert_approx_df_equality(df1, df2, 0.1)
```

The `assert_approx_df_equality` method is smart and will only perform approximate equality operations for floating point numbers in DataFrames.  It'll perform regular equality for strings and other types.

Let's perform an approximate equality comparison for two DataFrames that are not equal.

```python
def test_approx_df_equality_different():
    data1 = [
        (1.1, "a"),
        (2.2, "b"),
        (3.3, "c"),
        (None, None)
    ]
    df1 = spark.createDataFrame(data1, ["num", "letter"])

    data2 = [
        (1.1, "a"),
        (5.0, "b"),
        (3.3, "z"),
        (None, None)
    ]
    df2 = spark.createDataFrame(data2, ["num", "letter"])

    assert_approx_df_equality(df1, df2, 0.1)
```

Here's the pretty error message that's outputted:

![DataFramesNotEqualError](https://github.com/MrPowers/chispa/blob/main/images/dfs_not_approx_equal.png)

## Schema mismatch messages

DataFrame equality messages peform schema comparisons before analyzing the actual content of the DataFrames.  DataFrames that don't have the same schemas should error out as fast as possible.

Let's compare a DataFrame that has a string column an integer column with a DataFrame that has two integer columns to observe the schema mismatch message.

```python
def test_schema_mismatch_message():
    data1 = [
        (1, "a"),
        (2, "b"),
        (3, "c"),
        (None, None)
    ]
    df1 = spark.createDataFrame(data1, ["num", "letter"])

    data2 = [
        (1, 6),
        (2, 7),
        (3, 8),
        (None, None)
    ]
    df2 = spark.createDataFrame(data2, ["num", "num2"])

    assert_df_equality(df1, df2)
```

Here's the error message:

![SchemasNotEqualError](https://github.com/MrPowers/chispa/blob/main/images/schemas_not_approx_equal.png)

## DataFrame Generator

This module lets you create DataFrames, according to a schema, with fake synthetic data (using Faker) so that 
you can either use it in your tests or make use of property based testing (explained further down).

The starting point is the DataFrameGenerator data class, and it can be created by doing the following:

```python
df_gen: DataFrameGenerator = DataFrameGenerator(schema=schema)
```

Where the schema is a StructType object like the following:

```python
schema: StructType = StructType([
    StructField("expected_name", StringType(), True),
    StructField("int", IntegerType(), True),
    StructField("long", LongType(), True),
    StructField("byte", ByteType(), True),
    StructField("short", ShortType(), True),
    StructField("double", DoubleType(), True),
    StructField("float", FloatType(), True),
    StructField("decimal", DecimalType(), True),
    StructField("bool", BooleanType(), True),
    StructField("binary", BinaryType(), True),
    StructField("date", DateType(), True),
    StructField("timestamp", TimestampType(), True)
])
```

Those are the supported types right now, each of those types has a matching Faker provider in the `DEFAULT_CONFIG` dictionary:

```python
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
```

Calling the method `arbitrary_dataframes` from the DataFrameGenerator will give you a Python generator of 
DataFrames (default is 10 DataFrames) with rows filled with fake data (default is 10 rows).

For instance, the following code:

```python
schema: StructType = StructType([
    StructField("string", StringType(), True),
    StructField("int", IntegerType(), True),
    StructField("date", DateType(), True)
])
df_gen: DataFrameGenerator = DataFrameGenerator(schema=schema)
for df in df_gen.arbitrary_dataframes():
    df.show()
```

will result in:

```shell
+--------------------+-----------+----------+
|              string|        int|      date|
+--------------------+-----------+----------+
|athKDRmDyDoOFTtMyEpS|  208977570|1998-10-05|
|KsiKWhuWhrxwjIfZObWE|   36536111|2009-02-07|
|yBnbHFvtUaMITurvzgGa|-1150452234|1975-06-04|
|hJgwshOrnGuOVYSHiQvT| -394148922|1996-09-03|
|RRwPfXMSXfwPTpEbCCYd|-2126030849|2020-11-01|
|NxjZLvalBmUxlHCCdvRS| -868167137|2017-01-09|
|OCKxJFjEWnXFLTnmxlAL|  378510418|2004-09-08|
|FcPQoSKsaWtkVAtsHtmE|-1778979182|1976-06-08|
|gGLrGwLlHQUJgqLoHscd|-1707952693|1975-07-04|
|OQfOJfUAqYMdoDKyIODt| 2042919219|1974-08-18|
+--------------------+-----------+----------+

+--------------------+-----------+----------+
|              string|        int|      date|
+--------------------+-----------+----------+
|YdBiqoUTPchuiVVCToYb|  837577396|2011-10-20|
|ENUTmYcJbIlAHXdXrlcK| 1965683018|1999-06-11|
|qkYABZaLxKSTSKULvJUn|-1534538904|2007-07-21|
|IBekMJxdILbHrseyELjI| -778855686|1995-02-21|
|eOeDuqcyQrmMKyHsdIqi| 2062228449|2021-04-24|
|gvdnhvZEWHxjdVOCNVNO|  634606029|1988-07-28|
|XPooEkKLCsdDBBDPBxdw| 1147520365|2010-10-26|
|QuyvBSnhmNDFViNtZloD| -615531044|1988-11-11|
|wfNVuyjNwLOlIMILwEyY|    -438993|1998-05-08|
|vGqmOojchnEBiFUrIyEF|-1961143065|1995-10-13|
+--------------------+-----------+----------+
.
.
.
```

You can also make use of the `seed` parameter to always get the same results, could be beneficial in some test cases.

And, more importantly, you can make use of the `config` parameter to make sure the fake data in the DataFrames is as close 
to the actual data you use.

For instance, the following:

```python
schema: StructType = StructType([
    StructField("bank_account_number", StringType(), True),
    StructField("string", StringType(), True)
])

config: dict = {
    "string": {
        "data_type": StringType(),
        "provider": "random_element",
        "kwargs": {
            "elements": ('x', 'y')
        },
    },
    "bank_account_number": {
        "data_type": StringType(),
        "provider": "iban",
    }
}

df_gen: DataFrameGenerator = DataFrameGenerator(schema=schema, config=config)
for df in df_gen.arbitrary_dataframes():
    df.show(truncate=False)
```

will result in:

```shell
+----------------------+------+
|bank_account_number   |string|
+----------------------+------+
|GB05ZPOQ53062662223126|x     |
|GB30FBEP02205427369768|y     |
|GB77ZVZD72401097292467|y     |
|GB37ZAUF42921111575037|x     |
|GB94YOYW99106454150303|x     |
|GB45AHZD58341571053644|y     |
|GB49NIWO23000421077097|y     |
|GB57KKMR90126850543238|x     |
|GB36MJSE75788716032200|y     |
|GB09WQRG06056962875254|x     |
+----------------------+------+

+----------------------+------+
|bank_account_number   |string|
+----------------------+------+
|GB05DOHQ75315263055315|x     |
|GB97WKAZ86167865050998|x     |
|GB74FNSZ74818713531062|y     |
|GB09RNUK49954795362800|x     |
|GB52EKEI43974684705487|y     |
|GB71CMIO65098526908411|y     |
|GB21FRNR40256327200553|y     |
|GB78TPWY70848987416423|x     |
|GB72FOTB13893525853918|x     |
|GB88ZPWG41923933222632|y     |
+----------------------+------+
.
.
.
```

The other useful functionality of the DataFrameGenerator is that it can have a transformer function applied to the DataFrames,
perhaps you have a certain transformation that you would like to run over your DataFrame, if so you can pass this function to
the DataFrameGenerator, and it will then run it over the DataFrames and the iterator returned from the `arbitrary_dataframes`
method will now be DataFrames with that transformation applied.

For instance, the following:

```python
schema: StructType = StructType([
    StructField("string", StringType(), True),
    StructField("number1", IntegerType(), True),
    StructField("number2", IntegerType(), True),
])

def transformation(df: DataFrame) -> DataFrame:
    return df.withColumn("number_sum", col("number1") + col("number2")).limit(2)

df_gen: DataFrameGenerator = DataFrameGenerator(schema=schema, transformer=transformation)
for df in df_gen.arbitrary_dataframes():
    df.show(truncate=False)
```

of course, a simple transformation can be in lambda form as well:

```python
df_gen: DataFrameGenerator = DataFrameGenerator(schema=schema, transformer=lambda df: df.withColumn("number_sum", col("number1") + col("number2")).limit(2))
```



will result in:

```shell
+--------------------+-----------+----------+----------+
|string              |number1    |number2   |number_sum|
+--------------------+-----------+----------+----------+
|hfvXIxpHYWmeozQCgDdb|-404644089 |1391745093|987101004 |
|JMeuXcnlUBMabyYkckdL|-1019120536|1893116782|873996246 |
+--------------------+-----------+----------+----------+

+--------------------+-----------+-----------+----------+
|string              |number1    |number2    |number_sum|
+--------------------+-----------+-----------+----------+
|IuGgFufcEWilkamohglP|-1867212230|-1935661407|492093659 |
|rhsttgaKKcWcMVRCSGIk|1412079017 |16007381   |1428086398|
+--------------------+-----------+-----------+----------+

+--------------------+-----------+-----------+-----------+
|string              |number1    |number2    |number_sum |
+--------------------+-----------+-----------+-----------+
|RmihqKGAtoNxmofMNLms|-1729443426|377528902  |-1351914524|
|ZFScHJstlfOpJlvFFKmT|-2142767718|-1554653988|597545590  |
+--------------------+-----------+-----------+-----------+
.
.
.
```

## Property Based Testing Approach

Although far from mature, the code here is a good starting point and hopefully can only be made better, this approach
takes inspiration from the work done by [Holden Karau](https://github.com/holdenk) in the [spark-testing-base](https://github.com/holdenk/spark-testing-base) repository.
You can check the wiki for that [DataFrameGenerator](https://github.com/holdenk/spark-testing-base/wiki/DataFrameGenerator) and see how the scala solution is done there.
Here the approach to property based testing is similar.

There are two methods in the dataframe_generator module, `for_all` and `check_property` that you can make use of to do property checks.

For instance, the following simple test:

```python
def test_that_passes(self):
    schema: StructType = StructType([
        StructField("string", StringType(), True),
        StructField("number1", IntegerType(), True),
        StructField("number2", IntegerType(), True),
    ])

    def transformation(df: DataFrame) -> DataFrame:
        return df.withColumn("number_sum", col("number1") + col("number2")).limit(2)

    df_gen: DataFrameGenerator = DataFrameGenerator(schema=schema, transformer=transformation)

    new_data_schema: StructType = StructType([
        StructField("string", StringType(), True),
        StructField("number1", IntegerType(), True),
        StructField("number2", IntegerType(), True),
        StructField("number_sum", IntegerType(), True),
    ])

    property_results: Iterator[PropertyResult] = for_all(
        dfs=df_gen.arbitrary_dataframes(),
        property_to_check=lambda df: df.schema == new_data_schema and df.count() == 2
    )
    self.assertTrue(check_property(property_results=property_results))
```

will check the property defined in the `property_to_check` parameter in all the DataFrames and generate a report that is
then passed to the `check_property` that will either return True if all DataFrames conform to the property or it will raise
a `PropertyCheckException` and show the failed DataFrames in a pretty table.

Changing the property to check to:

```python
lambda df: df.schema == new_data_schema and df.count() == 3
```

Will cause the test to fail, and it will print out the DataFrames that failed the property check:

```shell
E           chispa.dataframe_generator.PropertyCheckException: Property Check failed:
E           +----------------------+------------+-------------+------------+
E           |        string        |  number1   |   number2   | number_sum |
E           +----------------------+------------+-------------+------------+
E           | eRNTppUYCUECmgCEDLUu | 1035096828 | -1427731638 | -392634810 |
E           | rQPPNXQuSGVuidEnWCxS | 774843839  | -1050333669 | -275489830 |
E           +----------------------+------------+-------------+------------+
E           +----------------------+-------------+-------------+-------------+
E           |        string        |   number1   |   number2   |  number_sum |
E           +----------------------+-------------+-------------+-------------+
E           | DYldTLJOXDsoLmpaSAUQ | -2040281200 |  -106962224 | -2147243424 |
E           | AjSnLZSGoGAjPcufpUgc |  709943750  | -1092598909 |  -382655159 |
E           +----------------------+-------------+-------------+-------------+
E           +----------------------+------------+------------+-------------+
E           |        string        |  number1   |  number2   |  number_sum |
E           +----------------------+------------+------------+-------------+
E           | BStGdFtsgZEyNSdAkLPr | 1526463691 | 1333610860 | -1434892745 |
E           | ddIYTJHNDWSXglhaTrnn | 482503049  | 1506843170 |  1989346219 |
E           +----------------------+------------+------------+-------------+
.
.
.
```

## Supported PySpark / Python versions

chispa currently supports PySpark 2.4+ and Python 3.5+.

Use chispa v0.8.2 if you're using an older Python version.

PySpark 2 support will be dropped when chispa 1.x is released.

## Benchmarks

TODO: Need to benchmark these methods vs. the spark-testing-base ones

## Vendored dependencies

These dependencies are vendored:

* [six](https://github.com/benjaminp/six)
* [PrettyTable](https://github.com/jazzband/prettytable)
* [Faker](https://github.com/joke2k/faker)

The dependencies are vendored to save you from dependency hell.

## Developing chispa on your local machine

You are encouraged to clone and/or fork this repo.

This project uses [Poetry](https://python-poetry.org/) for packaging and dependency management.

* Setup the virtual environment with `poetry install`
* Run the tests with `poetry run pytest tests`

Studying the codebase is a great way to learn about PySpark!

## Contributing

Anyone is encouraged to submit a pull request, open an issue, or submit a bug report.

We're happy to promote folks to be library maintainers if they make good contributions.
