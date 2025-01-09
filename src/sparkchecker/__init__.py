from pyspark.sql import DataFrame

from .app import sparkChecker

__all__ = [
    "sparkChecker",
]


DataFrame.sparkChecker = sparkChecker  # type: ignore
