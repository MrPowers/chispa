# chispa

![CI](https://github.com/MrPowers/chispa/workflows/CI/badge.svg?branch=master)

chispa provides PySpark testing helper methods that run quickly and output descriptive error messages to make development a breeze.

Fun fact: "chispa" means Spark in Spanish ;)

## Installation

Install the latest version with `pip install chispa`.

If you use Poetry, add this library as a development dependency with `poetry add chispa --dev`.

## Column equality

Suppose you have a function that removes all the whitespace in a string.

```python
def remove_non_word_characters(col):
    return F.regexp_replace(col, "[^\\w\\s]+", "")
```

Let's start by creating a `SparkSession` accessible via the `spark` variable so we can create DataFrames in our test suite.

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
  .master("local")
  .appName("chispa")
  .getOrCreate())
```

Create a DataFrame with a column that contains a lot of non-word characters, run the `remove_non_word_characters` function, and check that all these characters are removed with the chispa `assert_column_equality` method.

```python
import pytest

from chispa.column_comparer import *
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

Let's write another test that'll error out and inspect the test output to see how it's easy to debug the issue.

Here's the failing test.

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

This'll return a nicely formatted error message:

![ColumnsNotEqualError](https://github.com/MrPowers/chispa/blob/master/images/columns_not_equal_error.png)

We can see the `matt7` / `matt` row of data is what's causing the error cause it's highlighted in red.  The other rows are colored blue because they're equal.

## DataFrame equality

We can also test the `remove_non_word_characters` method by creating two DataFrames and verifying that they're equal.

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
        remove_non_word_characters(col("name"))
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

Let's write another test that'll return an error.

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

This'll return a nicely formatted error message:

![DataFramesNotEqualError](https://github.com/MrPowers/chispa/blob/master/images/dfs_not_equal_error.png)

### Row order independent DataFrame comparisons

here's how you can compare DataFrames, ignoring the row order (see [this commit](https://github.com/MrPowers/chispa/commit/e2469ef4bab509b50f6dd628734b45b468132e1b)):

```python
assert_df_equality(df1, df2, transforms=[lambda df: df.sort(df.columns)])
```

### Column order independent DataFrame comparisons

Here's how you can compare two DataFrames, ignoring the column order:

```python
assert_df_equality(df1, df2, transforms=[lambda df: df.select(sorted(df.columns))])
```

### Ignore nullability

TODO

### Allow NaN equality

TODO

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

![ColumnsNotEqualError](https://github.com/MrPowers/chispa/blob/master/images/columns_not_approx_equal.png)

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

![DataFramesNotEqualError](https://github.com/MrPowers/chispa/blob/master/images/dfs_not_approx_equal.png)

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

![SchemasNotEqualError](https://github.com/MrPowers/chispa/blob/master/images/schemas_not_approx_equal.png)

## Benchmarks

TODO: Need to benchmark these methods vs. the spark-testing-base ones

## Vendored dependencies

These dependencies are vendored:

* [six](https://github.com/benjaminp/six)
* [PrettyTable](https://github.com/jazzband/prettytable)

The dependencies are vendored to save you from dependency hell.

## Developing chispa on your local machine

You are encouraged to clone and/or fork this repo.

Clone the repo, run the test suite, and study the code.

This codebase is a great way to learn about PySpark!

## Contributing

Anyone is encouraged to submit a pull request, open an issue, or submit a bug report.

We're happy to promote folks to be library maintainers if they make good contributions.
