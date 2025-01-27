"""
File used for prototyping: src/proto.py.
Will be removed in the final version.
"""

from pyspark.sql import SparkSession

from sparkchecker import sparkChecker  # noqa: F401

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("examples/airline.csv", header=True, inferSchema=True)

df.sparkChecker(
    path="examples/expectations_airline.yaml",
    raise_error=True,
    print_log=True,
    write_file=True,
)
