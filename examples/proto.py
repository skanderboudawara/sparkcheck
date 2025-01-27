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

# Check the dataframe
df.sparkChecker(
    path="examples/expectations_airline.yaml",
    raise_error=True,
    print_log=True,
    write_file=True,
)
