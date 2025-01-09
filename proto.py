"""
File used for prototyping: src/proto.py.
Will be removed in the final version.
"""

from pyspark.sql import SparkSession

from sparkchecker import sparkChecker  # noqa: F401

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("examples/airline_1m.csv", header=True, inferSchema=True)
df.sparkChecker("examples/expectations_airline.yaml")
