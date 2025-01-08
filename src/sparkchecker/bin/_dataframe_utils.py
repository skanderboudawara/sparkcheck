from abc import ABC, abstractmethod
from typing import Literal

from pyspark.sql import DataFrame

from sparkchecker.bin._constants import OPERATOR_MAP
from sparkchecker.bin._utils import _check_operator


class _DataFrameExpectation(ABC):
    def __init__(self, df: DataFrame= None, *args, **kwargs) -> None:
        self.df: DataFrame = df

    @property
    @abstractmethod
    def predicate(self) -> bool: ...




class _IsEmpty(_DataFrameExpectation):
    def __init__(self, df: DataFrame= None, *args, **kwargs) -> None:
        """
        This class checks if a DataFrame is empty.

        :param df: (DataFrame), the DataFrame to check

        :return: None
        """
        super().__init__(df)

    @property
    def predicate(self) -> bool:
        """
        This method returns the predicate.

        :param None:

        :return: (bool), the predicate
        """
        return self.df.isEmpty(), 0


class _IsNotEmpty(_DataFrameExpectation):
    def __init__(self, df: DataFrame= None, *args, **kwargs) -> None:
        """
        This class checks if a DataFrame is empty.

        :param df: (DataFrame), the DataFrame to check

        :return: None
        """
        super().__init__(df)

    @property
    def predicate(self) -> bool:
        """
        This method returns the predicate.

        :param None:

        :return: (bool), the predicate
        """
        return not (self.df.isEmpty()), 0


class _CountThreshold(_DataFrameExpectation):
    def __init__(self, constraint: int, operator: str,  df: DataFrame = None, *args, **kwargs) -> None:
        """
        This class compares the count of a DataFrame to a constraint.

        :param df: (DataFrame), the DataFrame to check

        :param constraint: (int), the constraint to check

        :return: None
        """
        _check_operator(operator)
        self.constraint = constraint
        self.operator = operator
        super().__init__(df)

    @property
    def predicate(self) -> bool:
        """
        This method returns the predicate.

        :param None:

        :return: (bool), the predicate
        """
        count = self.df.count()
        # Convert the threshold to a literal value and apply the operator
        check = OPERATOR_MAP[self.operator](count, self.constraint)
        if check:
            return True, count
        return False, count


class _PartitionsCount(_DataFrameExpectation):
    def __init__(self, constraint: int, operator: str, df: DataFrame = None,  *args, **kwargs) -> None:
        """
        This class compares the number of partitions of a DataFrame to a constraint.

        :param df: (DataFrame), the DataFrame to check

        :param constraint: (int), the constraint to check

        :return: None
        """
        _check_operator(operator)
        self.constraint = constraint
        self.operator = operator
        super().__init__(df)

    @property
    def predicate(self) -> bool:
        """
        This method returns the predicate.

        :param None:

        :return: (bool), the predicate
        """
        rdd_count = self.df.rdd.getNumPartitions()
        # Convert the threshold to a literal value and apply the operator
        check_count = OPERATOR_MAP[self.operator](
            rdd_count,
            self.constraint,
        )
        if check_count:
            return True, rdd_count
        return False, rdd_count
