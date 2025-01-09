from pyspark.sql import DataFrame

from .sparkchecker.app import sparkChecker

__all__ = [
    "sparkChecker",
]

DataFrame.sparkChecker = sparkChecker  # type: ignore
