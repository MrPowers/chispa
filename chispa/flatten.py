from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql.functions import col, explode_outer, map_keys
from pyspark.sql.types import ArrayType, MapType, StructType

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import DataType


def _complex_fields(schema: StructType) -> dict[str, DataType]:
    return {
        field.name: field.dataType
        for field in schema.fields
        if isinstance(field.dataType, (StructType, ArrayType, MapType))
    }


def flatten_dataframe(df: DataFrame, sep: str = "_") -> DataFrame:
    if sep == ".":
        raise ValueError("`sep` must not be '.', it conflicts with Spark's struct field accessor")

    remaining = _complex_fields(df.schema)
    while remaining:
        col_name, dtype = next(iter(remaining.items()))

        if isinstance(dtype, StructType):
            expanded = [col(f"`{col_name}`.`{f.name}`").alias(f"{col_name}{sep}{f.name}") for f in dtype.fields]
            df = df.select("*", *expanded).drop(col_name)

        elif isinstance(dtype, ArrayType):
            df = df.withColumn(col_name, explode_outer(col_name))

        elif isinstance(dtype, MapType):
            keys_rows = df.select(explode_outer(map_keys(col(col_name))).alias("k")).distinct().collect()
            keys = [row["k"] for row in keys_rows if row["k"] is not None]
            key_cols = [col(col_name).getItem(k).alias(f"{col_name}{sep}{k}") for k in keys]
            df = df.select(*[c for c in df.columns if c != col_name], *key_cols)

        remaining = _complex_fields(df.schema)

    return df
