from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, StructType


def flatten_dataframe(df: DataFrame, sep: str = "_") -> DataFrame:
    """Flatten a nested DataFrame by expanding StructType, ArrayType, and MapType columns.

    This function recursively flattens nested structures in a DataFrame, converting
    complex types into flat columns with names separated by the specified delimiter.

    Parameters
    ----------
    df : DataFrame
        The input DataFrame to flatten.
    sep : str, optional
        Delimiter for flattened column names. Default is "_".
        Note: Do not use "." as the separator, as it won't work correctly with
        nested DataFrames with more than one level.

    Returns
    -------
    DataFrame
        A flattened DataFrame with all nested structures expanded.

    Notes
    -----
    - StructType fields are expanded into individual columns
    - ArrayType fields are exploded to add array elements as rows
    - MapType fields are expanded by extracting all keys as columns
    - Flattening MapType requires finding every key in the column, which can be slow
    - The function processes fields iteratively until no complex types remain

    Examples
    --------
    Flatten a DataFrame with nested struct fields:

    >>> data = [
    ...     {"id": 1, "name": "Cole", "fitness": {"height": 130, "weight": 60}},
    ...     {"id": 2, "name": "Faye", "fitness": {"height": 130, "weight": 60}},
    ... ]
    >>> df = spark.createDataFrame(data)
    >>> flat_df = flatten_dataframe(df, sep=":")
    >>> flat_df.columns
    ['id', 'name', 'fitness:height', 'fitness:weight']

    Flatten a DataFrame with map fields:

    >>> data = [
    ...     {"state": "Florida", "info": {"governor": "Rick Scott"}},
    ...     {"state": "Ohio", "info": {"governor": "John Kasich"}},
    ... ]
    >>> df = spark.createDataFrame(data)
    >>> flat_df = flatten_dataframe(df, sep=":")
    >>> flat_df.columns
    ['state', 'info:governor']

    Flatten a DataFrame with array fields:

    >>> data = [
    ...     {"name": "John", "scores": [85, 90, 95]},
    ...     {"name": "Jane", "scores": [88, 92, 94]},
    ... ]
    >>> df = spark.createDataFrame(data)
    >>> flat_df = flatten_dataframe(df)
    >>> "scores" in flat_df.columns
    False
    """
    # Compute Complex Fields (Arrays, Structs and MapTypes) in Schema
    complex_fields: dict[str, StructType | ArrayType | MapType] = dict(
        [
            (field.name, field.dataType)
            for field in df.schema.fields
            if isinstance(field.dataType, ArrayType | StructType | MapType)
        ]
    )

    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]

        # If StructType then convert all sub-element to columns.
        # i.e. flatten structs
        if isinstance(complex_fields[col_name], StructType):
            expanded = [
                F.col(col_name + "." + k).alias(col_name + sep + k)
                for k in [n.name for n in complex_fields[col_name]]
            ]
            df = df.select("*", *expanded).drop(col_name)

        # If ArrayType then add the Array Elements as Rows using the explode function
        # i.e. explode Arrays
        elif isinstance(complex_fields[col_name], ArrayType):
            df = df.withColumn(col_name, F.explode_outer(col_name))

        # If MapType then convert all sub-element to columns.
        # i.e. flatten maps
        elif isinstance(complex_fields[col_name], MapType):
            keys_df = df.select(F.explode_outer(F.map_keys(F.col(col_name)))).distinct()
            keys = [row[0] for row in keys_df.collect()]
            key_cols = [
                F.col(col_name).getItem(f).alias(str(col_name + sep + f)) for f in keys
            ]
            drop_column_list = [col_name]
            df = df.select(
                [c for c in df.columns if c not in drop_column_list] + key_cols
            )

        # Recompute remaining Complex Fields in Schema
        complex_fields = dict(
            [
                (field.name, field.dataType)
                for field in df.schema.fields
                if isinstance(field.dataType, ArrayType | StructType | MapType)
            ]
        )

    return df
