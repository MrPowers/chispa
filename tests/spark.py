from __future__ import annotations

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("chispa").getOrCreate()
