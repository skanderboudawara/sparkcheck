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
    def __init__(self, cname: Union[str, Column]) -> None:
        self.column: Column = cname

    @property
    @abstractmethod
    def predicate(self) -> Column: ...


class _NonNullColumn(_ColumnsExpectations):
    def __init__(self, column: Union[str, Column], constraint,  *args, **kwargs) -> None:
        """
        This class checks if a column is not null.

        :param column: (Union[str, Column]), the column to check

        :return: None
        """
        self.constraint = constraint
        super().__init__(cname=column)

    @property
    def predicate(self) -> Column:
        """
        This method returns the predicate.

        :param None:

        :return: (Column), the predicate
        """
        self.column = _str_to_col(self.column)
        if self.constraint:
            return self.column.isNotNull()
        return self.column.isNull()


class _NullColumn(_ColumnsExpectations):
    def __init__(self, column: Union[str, Column], constraint,  *args, **kwargs) -> None:
        """
        This class checks if a column is null.

        :param column: (Union[str, Column]), the column to check

        :return: None
        """
        self.constraint = constraint
        super().__init__(cname=column)

    @property
    def predicate(self) -> Column:
        """
        This method returns the predicate.

        :param None:

        :return: (Column), the predicate
        """
        self.column = _str_to_col(self.column)
        if self.constraint:
            return self.column.isNull()
        return self.column.isNotNull()


class _RlikeColumn(_ColumnsExpectations):
    def __init__(self, column: Union[str, Column], constraint: str,  *args, **kwargs) -> None:
        """
        This class checks if a column matches a pattern.

        :param column: (Union[str, Column]), the column to check

        :param constraint: (str), the constraint to match

        :return: None
        """
        super().__init__(cname=column)
        self.constraint = constraint

    @property
    def predicate(self) -> Column:
        """
        This method returns the predicate.

        :param None:

        :return: (Column), the predicate
        """
        self.column = _str_to_col(self.column)
        return self.column.rlike(self.constraint)


class _IsInColumn(_ColumnsExpectations):
    def __init__(
        self,
        column: Union[str, Column],
        constraint: Union[Column, str, list[Column], list[str]],
         *args, **kwargs
    ) -> None:
        """
        This class checks if a column is in an array.

        :param column: (Union[str, Column]), the column to check

        :param array: (Union[Column, str, list[Column], list[str]]), the array to check

        :return: None
        """
        super().__init__(cname=column)
        self.constraint = constraint

    @property
    def predicate(self) -> Column:
        """
        This method returns the predicate.

        :param None:

        :return: (Column), the predicate
        """
        self.constraint = _args_to_list_cols(self.constraint, is_lit=True)
        self.column = _str_to_col(self.column)
        return self.column.isin(*self.constraint)


class _ColumnCompare(_ColumnsExpectations):
    def __init__(
        self,
        column: Union[str, Column],
        constraint: Union[str, float],
        operator: str,
         *args, **kwargs
    ) -> None:
        """
        This class compares a column to a constraint.

        :param column: (Union[str, Column]), the column to compare

        :param constraint: (Union[str, float]), the constraint to compare

        :param operator: (str), the operator to use
        """
        _check_operator(operator)
        self.operator = operator
        self.constraint= constraint
        super().__init__(cname=column)

    @property
    def predicate(self) -> Column:
        """
        This method returns the predicate.

        :param None:

        :return: (Column), the predicate
        """
        self.column = _str_to_col(self.column)
        self.constraint = _str_to_col(self.constraint, is_lit=True)

        # Convert the constraint to a literal value and apply the operator
        return OPERATOR_MAP[self.operator](self.column, self.constraint)
