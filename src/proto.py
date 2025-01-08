"""
File used for prototyping: src/proto.py.

Will be removed in the final version.
"""
from sparkchecker import sparkChecker
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("examples/airline.csv", header=True, inferSchema=True)
df.sparkChecker("examples/expectations_airline.yaml")
