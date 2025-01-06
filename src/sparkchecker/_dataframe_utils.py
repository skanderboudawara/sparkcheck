from abc import ABC, abstractmethod
from typing import Literal

from pyspark.sql import DataFrame

from sparkchecker._constants import OPERATOR_MAP
from sparkchecker._utils import _check_operator


class _DataFrameExpectation(ABC):
    def __init__(self, df: DataFrame) -> None:
        self.df: DataFrame = df

    @property
    @abstractmethod
    def predicate(self) -> bool: ...


class _IsEmpty(_DataFrameExpectation):
    def __init__(self, df: DataFrame) -> None:
        """
        This class checks if a DataFrame is empty.

        :param df: (DataFrame), the DataFrame to check

        :return: None
        """
        super().__init__(df)

    @property
    def predicate(self) -> Literal["isEmpty"]:
        """
        This method returns the predicate.

        :param None:

        :return: (bool), the predicate
        """
        return self.df.isEmpty()


class _CountThreshold(_DataFrameExpectation):
    def __init__(self, df: DataFrame, threshold: int, operator: str) -> None:
        """
        This class compares the count of a DataFrame to a threshold.

        :param df: (DataFrame), the DataFrame to check

        :param threshold: (int), the threshold to check

        :return: None
        """
        _check_operator(operator)
        self.threshold = threshold
        self.operator = operator
        super().__init__(df)

    @property
    def predicate(self) -> bool:
        """
        This method returns the predicate.

        :param None:

        :return: (bool), the predicate
        """
        # Convert the threshold to a literal value and apply the operator
        return OPERATOR_MAP[self.operator](self.df.count(), self.threshold)


class _PartitionsCount(_DataFrameExpectation):
    def __init__(self, df: DataFrame, threshold: int, operator: str) -> None:
        """
        This class compares the number of partitions of a DataFrame to a threshold.

        :param df: (DataFrame), the DataFrame to check

        :param threshold: (int), the threshold to check

        :return: None
        """
        _check_operator(operator)
        self.threshold = threshold
        self.operator = operator
        super().__init__(df)

    @property
    def predicate(self) -> bool:
        """
        This method returns the predicate.

        :param None:

        :return: (bool), the predicate
        """
        # Convert the threshold to a literal value and apply the operator
        return OPERATOR_MAP[self.operator](
            self.df.rdd.getNumPartitions(),
            self.threshold,
        )
