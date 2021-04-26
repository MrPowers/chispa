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
    assert_column_equality(df, "num1", "num2", precision=0.1)
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
    assert_column_equality(df, "num1", "num2", precision=0.1)
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

    assert_df_equality(df1, df2, precision=0.1)
```

The `assert_df_equality` method has a `precision` parameter that let's the user control the absolute tolerance of any floating point errors that are accepted by the assertion method. It is smart and will only perform approximate equality operations for floating point numbers in DataFrames.  It'll perform regular equality for strings and other types.

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

    assert_df_equality(df1, df2, precision=0.1)
```

Here's the pretty error message that's outputted:

![DataFramesNotEqualError](https://github.com/MrPowers/chispa/blob/main/images/dfs_not_approx_equal.png)

## Schema mismatch messages

DataFrame equality messages perform schema comparisons before analyzing the actual content of the DataFrames.  DataFrames that don't have the same schemas should error out as fast as possible.

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

## Benchmarks

TODO: Need to benchmark these methods vs. the spark-testing-base ones

## Vendored dependencies

These dependencies are vendored:

* [six](https://github.com/benjaminp/six)
* [PrettyTable](https://github.com/jazzband/prettytable)

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
