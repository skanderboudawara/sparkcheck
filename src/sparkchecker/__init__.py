from .app import sparkChecker
from pyspark.sql import DataFrame

__all__ = [
    "sparkChecker",
]


DataFrame.sparkChecker = sparkChecker  # type: ignore