# chispa

chispa provides PySpark testing helper methods that run quickly and output descriptive error messages to make development a breeze.

Fun fact: "chispa" means Spark in Spanish ;)

## Column equality

Suppose you have a function that removes all the whitespace in a string.

```python
def remove_non_word_characters(col):
    return F.regexp_replace(col, "[^\\w\\s]+", "")
```

Let's start by creating a `SparkSession` accessible via the `spark` variable so we can create DataFrames in our test suite.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
  .master("local") \
  .appName("chispa") \
  .getOrCreate()
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
    df = spark.createDataFrame(data, ["name", "expected_name"])\
        .withColumn("clean_name", remove_non_word_characters(F.col("name")))
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
    df = spark.createDataFrame(data, ["name", "expected_name"])\
        .withColumn("clean_name", remove_non_word_characters(F.col("name")))
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

## Approximate column equality



## Approximate DataFrame equality



## Schema error messages



## Benchmarks



## Limitations



## Developing on your local machine

We encourage cloning and forking this repo.

Clone the repo, run the test suite, and study the code.  This repo is a great way to learn about PySpark!

## Contributing

Anyone is encouraged to submit a pull request.



