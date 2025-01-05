from abc import ABC, abstractmethod
from typing import Literal

from pyspark.sql import DataFrame

from sparkchecker._constants import OPERATOR_MAP


class _DataFrameExpectation(ABC):
    def __init__(self, df: DataFrame) -> None:
        self.df: DataFrame = DataFrame

    @property
    @abstractmethod
    def predicate(self) -> bool: ...


class _IsEmpty(_DataFrameExpectation):
    def __init__(self, df: DataFrame) -> None:
        super().__init__(df)

    @property
    def predicate(self) -> Literal["isEmpty"]:
        return self.df.isEmpty()


class _CountThreshold(_DataFrameExpectation):
    def __init__(self, df: DataFrame, threshold: int, operator: str) -> None:
        super().__init__(df)
        self.threshold = threshold
        self.operator = operator

        if operator not in OPERATOR_MAP:
            raise ValueError(
                f"Invalid operator: {operator}. Must be one of {list(OPERATOR_MAP.keys())}",
            )

    @property
    def predicate(self) -> bool:
        # Convert the threshold to a literal value and apply the operator
        return OPERATOR_MAP[self.operator](self.df.count(), self.threshold)


class _PartitionsCount(_DataFrameExpectation):
    def __init__(self, df: DataFrame, threshold: int, operator: str) -> None:
        super().__init__(df)
        self.threshold = threshold
        self.operator = operator

        if operator not in OPERATOR_MAP:
            raise ValueError(
                f"Invalid operator: {operator}. Must be one of {list(OPERATOR_MAP.keys())}",
            )

    @property
    def predicate(self) -> bool:
        # Convert the threshold to a literal value and apply the operator
        return OPERATOR_MAP[self.operator](
            self.df.rdd.getNumPartitions(),
            self.threshold,
        )
