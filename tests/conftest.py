from pyspark.sql import SparkSession
import pytest


@pytest.fixture(scope="session")
def spark_session():
    spark = (
        SparkSession.builder.appName("pytest")
        .getOrCreate()
    )
    spark.catalog.clearCache()
    yield spark
    spark.stop()
