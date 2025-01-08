"""
File used for prototyping: src/proto.py.

Will be removed in the final version.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame(
    schema=["passenger_name", "passenger_age", "passenger_origins"],
    data=[
        ["John Doe", 30, "New York"],
        ["Jane Doe", 25, "Los Angeles"],
        ["Alice Smith", 28, "Chicago"],
        ["Bob Johnson", 35, "Houston"],
        ["Charlie Brown", 22, "Phoenix"],
    ],
)
df.sparkChecker("examples/check_df.yaml")
