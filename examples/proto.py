"""
File used for prototyping the package.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from sparkchecker import sparkChecker  # noqa: F401

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("examples/airline.csv", header=True, inferSchema=True)

# Different transformation
df = df.withColumn(
    "passengers_country_bis",
    col("passengers_country"),
)

import cProfile
import pstats

with cProfile.Profile() as pr:
    df.sparkChecker(
        path="examples/expectations_airline.yaml",
        raise_error=False,
        print_log=True,
        write_file=True,
    )

stats = pstats.Stats(pr)
stats.sort_stats(pstats.SortKey.TIME)
stats.dump_stats(filename="examples/profile_stats.prof")