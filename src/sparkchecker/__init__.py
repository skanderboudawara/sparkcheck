from pyspark.sql import DataFrame

from .module.sparkchecker import sparkChecker

__all__ = [
    "sparkChecker",
]

DataFrame.sparkChecker = sparkChecker  # type: ignore
