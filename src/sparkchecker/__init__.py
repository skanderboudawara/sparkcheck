from pyspark.sql import DataFrame

from .sparkchecker import sparkChecker

__all__ = [
    "sparkChecker",
]


DataFrame.sparkChecker = sparkChecker  # type: ignore
