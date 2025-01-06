from abc import ABC, abstractmethod
from typing import Union

from pyspark.sql import Column
from pyspark.sql.functions import lit

from sparkchecker.bin._utils import (
    _args_to_list_cols,
    _check_operator,
    _str_to_col,
)
from sparkchecker.bin._constants import OPERATOR_MAP


class _ColumnsExpectations(ABC):
    def __init__(self, column: Union[str, Column]) -> None:
        self.column: Column = column

    @property
    @abstractmethod
    def predicate(self) -> Column: ...


class _NonNullColumn(_ColumnsExpectations):
    def __init__(self, column: Union[str, Column]) -> None:
        """
        This class checks if a column is not null.

        :param column: (Union[str, Column]), the column to check

        :return: None
        """
        super().__init__(_str_to_col(column))

    @property
    def predicate(self) -> Column:
        """
        This method returns the predicate.

        :param None:

        :return: (Column), the predicate
        """
        return self.column.isNotNull()


class _NullColumn(_ColumnsExpectations):
    def __init__(self, column: Union[str, Column]) -> None:
        """
        This class checks if a column is null.

        :param column: (Union[str, Column]), the column to check

        :return: None
        """
        super().__init__(_str_to_col(column))

    @property
    def predicate(self) -> Column:
        """
        This method returns the predicate.

        :param None:

        :return: (Column), the predicate
        """
        return self.column.isNull()


class _RlikeColumn(_ColumnsExpectations):
    def __init__(self, column: Union[str, Column], pattern: str) -> None:
        """
        This class checks if a column matches a pattern.

        :param column: (Union[str, Column]), the column to check

        :param pattern: (str), the pattern to match

        :return: None
        """
        super().__init__(_str_to_col(column))
        self.pattern = pattern

    @property
    def predicate(self) -> Column:
        """
        This method returns the predicate.

        :param None:

        :return: (Column), the predicate
        """
        return self.column.rlike(self.pattern)


class _IsInColumn(_ColumnsExpectations):
    def __init__(
        self,
        column: Union[str, Column],
        array: Union[Column, str, list[Column], list[str]],
    ) -> None:
        """
        This class checks if a column is in an array.

        :param column: (Union[str, Column]), the column to check

        :param array: (Union[Column, str, list[Column], list[str]]), the array to check

        :return: None
        """
        super().__init__(_str_to_col(column))
        self.array = _args_to_list_cols(array, is_lit=True)

    @property
    def predicate(self) -> Column:
        """
        This method returns the predicate.

        :param None:

        :return: (Column), the predicate
        """
        return self.column.isin(*self.array)


class _ColumnCompare:
    def __init__(
        self,
        column: Union[str, Column],
        threshold: Union[str, float],
        operator: str,
    ) -> None:
        """
        This class compares a column to a threshold.

        :param column: (Union[str, Column]), the column to compare

        :param threshold: (Union[str, float]), the threshold to compare

        :param operator: (str), the operator to use
        """
        _check_operator(operator)
        self.threshold = _str_to_col(threshold, is_lit=True)
        self.operator = operator
        super().__init__(_str_to_col(column))

    @property
    def predicate(self) -> Column:
        """
        This method returns the predicate.

        :param None:

        :return: (Column), the predicate
        """
        # Convert the threshold to a literal value and apply the operator
        return OPERATOR_MAP[self.operator](self.column, lit(self.threshold))
