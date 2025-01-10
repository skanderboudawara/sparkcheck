from pyspark.sql import SparkSession
import pytest


@pytest.fixture(scope="session")
def spark_session():
    spark = (
        SparkSession.builder.appName("pytest")
        .config("spark.sql.shuffle.partitions", 1)
        .config("spark.num.executors", 1)
        .config("spark.executor.cores", 1)
        .config("spark.executor.memory", "512m")
        .config("spark.driver.memory", "512m")
        .getOrCreate()
    )
    yield spark
    spark.stop()
