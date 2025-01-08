from abc import ABC, abstractmethod
from typing import Any, Union

from pyspark.sql import Column

from sparkchecker.bin._decorators import validate_predicate
from sparkchecker.bin._constants import OPERATOR_MAP
from sparkchecker.bin._utils import (
    _args_to_list_cols,
    _check_operator,
    _col_to_name,
    _str_to_col,
)


class _ColumnsExpectations(ABC):
    def __init__(self, cname: Union[str, Column]) -> None:
        self.column: Column = cname

    @property
    @abstractmethod
    def predicate(self) -> Column: ...


class _NonNullColumn(_ColumnsExpectations):
    def __init__(
        self,
        column: Union[str, Column],
        constraint: bool,
        **kwargs: Any,
    ) -> None:
        """
        This class checks if a column is not null.

        :param column: (Union[str, Column]), the column to check

        :param constraint: (bool), the constraint to check

        :return: None
        """
        if not isinstance(constraint, bool):
            raise TypeError(
                "Argument is not_null `constraint` must be of type bool but got: ",
                type(constraint),
            )
        self.constraint = constraint
        super().__init__(cname=column)

    @property
    @validate_predicate
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
    def __init__(
        self,
        column: Union[str, Column],
        constraint: bool,
        **kwargs: Any,
    ) -> None:
        """
        This class checks if a column is null.

        :param column: (Union[str, Column]), the column to check

        :param constraint: (bool), the constraint to check

        :return: None
        """
        if not isinstance(constraint, bool):
            raise TypeError(
                "Argument for is_null `constraint` must be of type bool but got: ",
                type(constraint),
            )
        self.constraint = constraint
        super().__init__(cname=column)

    @property
    @validate_predicate
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
    def __init__(
        self,
        column: Union[str, Column],
        constraint: str,
        **kwargs: Any,
    ) -> None:
        """
        This class checks if a column matches a pattern.

        :param column: (Union[str, Column]), the column to check

        :param constraint: (str), the constraint to match

        :return: None
        """
        super().__init__(cname=column)
        if not isinstance(constraint, str):
            raise TypeError(
                "Argument pattern `constraint` must be of type bool but got: ",
                type(constraint),
            )
        self.constraint = constraint

    @property
    @validate_predicate
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
        message: Union[str, None] = None,
        **kwargs: Any,
    ) -> None:
        """
        This class checks if a column is in an array.

        :param column: (Union[str, Column]), the column to check

        :param array: (Union[Column, str, list[Column], list[str]]), the array to check

        :return: None
        """
        super().__init__(cname=column)
        if not isinstance(constraint, (str, Column, list)):
            raise TypeError(
                "Argument for in `constraint` must be of type \
                    Union[Column, str, list[Column], list[str]] but got: ",
                type(constraint),
            )
        self.constraint = constraint

    @property
    @validate_predicate
    def predicate(self) -> Column:
        """
        This method returns the predicate.

        :param None:

        :return: (Column), the predicate
        """
        self.message = (
            f"Column {_col_to_name(self.column)} <$is_or_not> in the array: {', '.join(self.constraint)}"
            if self.message
            else self.message
        )
        self.constraint = _args_to_list_cols(self.constraint, is_lit=True)
        self.column = _str_to_col(self.column)
        return self.column.isin(*self.constraint)


class _ColumnCompare(_ColumnsExpectations):
    def __init__(
        self,
        column: Union[str, Column],
        constraint: Union[str, float],
        operator: str,
        **kwargs: Any,
    ) -> None:
        """
        This class compares a column to a constraint.

        :param column: (Union[str, Column]), the column to compare

        :param constraint: (Union[str, float]), the constraint to compare

        :param operator: (str), the operator to use
        """
        _check_operator(operator)
        self.operator = operator
        if not isinstance(constraint, (str, float, int)):
            raise TypeError(
                "Argument for column comparison `constraint` must be of type \
                    Union[str, float] but got: ",
                type(constraint),
            )
        self.constraint = constraint
        super().__init__(cname=column)

    @property
    @validate_predicate
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
