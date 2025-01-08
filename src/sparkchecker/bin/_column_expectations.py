from abc import ABC, abstractmethod
from typing import Union

from pyspark.sql import Column, DataFrame

from sparkchecker.bin._constants import OPERATOR_MAP
from sparkchecker.bin._decorators import (
    check_column_exist,
    check_message,
    validate_expectation,
)
from sparkchecker.bin._utils import (
    _check_operator,
    args_to_list_cols,
    col_to_name,
    str_to_col,
)


def evaluate_expectation(df: DataFrame, expectation: Column) -> tuple[bool, int, dict]:
    if not isinstance(expectation, Column):
        raise TypeError(
            "Argument `expectation` must be of type Column but got: ",
            type(expectation),
        )
    if not isinstance(df, DataFrame):
        raise TypeError(
            "Argument `df` must be of type DataFrame but got: ",
            type(df),
        )
    df = df.filter(expectation)
    first_failed_row = df.first()
    check = bool(not first_failed_row)
    count_cases = 0
    if check:
        count_cases = df.count()
    return check, count_cases, first_failed_row.asDict()


class ColumnsExpectations(ABC):
    @check_message
    def __init__(
        self, col_name: Union[str, Column], message: Union[str, None] = None, **kargs,
    ) -> None:
        self.column: Column = col_name
        self.message: str = message

    @property
    @abstractmethod
    def constraint(self) -> Column: ...

    @abstractmethod
    def expectation(self, df: DataFrame) -> Column: ...


class NonNullColumn(ColumnsExpectations):

    @check_message
    def __init__(
        self,
        column: Union[str, Column],
        value: bool,
        message: Union[str, None] = None,
        **kargs,
    ) -> None:
        """
        This class checks if a column is not null.

        :param column: (Union[str, Column]), the column to check

        :param value: (bool), the value to check

        :return: None
        """
        super().__init__(column, message)
        if not isinstance(value, bool):
            raise TypeError(
                "Argument is not_null `value` must be of type bool but got: ",
                type(value),
            )
        self.value = value

    @property
    def constraint(self) -> Column:
        self.column = str_to_col(self.column)
        if self.value:
            return self.column.isNotNull()
        return self.column.isNull()

    @validate_expectation
    @check_column_exist
    def expectation(self, df: DataFrame) -> Column:
        """
        This method returns the expectation.

        :param None:

        :return: (Column), the expectation
        """
        return evaluate_expectation(df, self.value)


class NullColumn(ColumnsExpectations):
    @check_message
    def __init__(
        self,
        column: Union[str, Column],
        value: bool,
        message: Union[str, None] = None,
        **kargs,
    ) -> None:
        """
        This class checks if a column is null.

        :param column: (Union[str, Column]), the column to check

        :param value: (bool), the value to check

        :return: None
        """
        super().__init__(column, message)
        if not isinstance(value, bool):
            raise TypeError(
                "Argument for is_null `value` must be of type bool but got: ",
                type(value),
            )
        self.value = value

    @property
    def constraint(self) -> Column:
        self.column = str_to_col(self.column)
        if self.value:
            return self.column.isNull()
        return self.column.isNotNull()

    @validate_expectation
    @check_column_exist
    def expectation(self, df: DataFrame) -> Column:
        """
        This method returns the expectation.

        :param None:

        :return: (Column), the expectation
        """
        return evaluate_expectation(df, self.value)


class RlikeColumn(ColumnsExpectations):
    @check_message
    def __init__(
        self,
        column: Union[str, Column],
        value: str,
        message: Union[str, None] = None,
        **kargs,
    ) -> None:
        """
        This class checks if a column matches a pattern.

        :param column: (Union[str, Column]), the column to check

        :param value: (str), the value to match

        :return: None
        """
        super().__init__(column, message)
        if not isinstance(value, str):
            raise TypeError(
                "Argument pattern `value` must be of type bool but got: ",
                type(value),
            )
        self.value = value

    @property
    def constraint(self) -> Column:
        self.column = str_to_col(self.column)
        return self.column.rlike(self.value)

    @validate_expectation
    @check_column_exist
    def expectation(self, df: DataFrame) -> Column:
        """
        This method returns the expectation.

        :param None:

        :return: (Column), the expectation
        """
        return evaluate_expectation(df, self.value)


class IsInColumn(ColumnsExpectations):
    @check_message
    def __init__(
        self,
        column: Union[str, Column],
        value: Union[Column, str, list[Column], list[str]],
        message: Union[str, None] = None,
        **kargs,
    ) -> None:
        """
        This class checks if a column is in an array.

        :param column: (Union[str, Column]), the column to check

        :param array: (Union[Column, str, list[Column], list[str]]), the array to check

        :return: None
        """
        super().__init__(column, message)
        if not isinstance(value, (str, Column, list)):
            raise TypeError(
                "Argument for in `value` must be of type \
                    Union[Column, str, list[Column], list[str]] but got: ",
                type(value),
            )
        self.value = value

    @property
    def constraint(self) -> Column:
        self.value = args_to_list_cols(self.value, is_col=False)
        self.column = str_to_col(self.column)
        return self.column.isin(*self.value)

    @validate_expectation
    @check_column_exist
    def expectation(self, df: DataFrame) -> Column:
        """
        This method returns the expectation.

        :param None:

        :return: (Column), the expectation
        """
        return evaluate_expectation(df, self.value)


class ColumnCompare(ColumnsExpectations):
    @check_message
    def __init__(
        self,
        column: Union[str, Column],
        value: Union[str, float],
        operator: str,
        message: Union[str, None] = None,
        **kargs,
    ) -> None:
        """
        This class compares a column to a value.

        :param column: (Union[str, Column]), the column to compare

        :param value: (Union[str, float]), the value to compare

        :param operator: (str), the operator to use
        """
        super().__init__(column, message)
        _check_operator(operator)
        self.operator = operator
        if not isinstance(value, (str, float, int)):
            raise TypeError(
                "Argument for column comparison `value` must be of type \
                    Union[str, float] but got: ",
                type(value),
            )
        self.value = value

    @property
    def constraint(self) -> Column:
        self.column = str_to_col(self.column)
        return OPERATOR_MAP[self.operator](self.column, self.value)

    @validate_expectation
    @check_column_exist
    def expectation(self, df: DataFrame) -> Column:
        """
        This method returns the expectation.

        :param None:

        :return: (Column), the expectation
        """
        is_col = col_to_name(str(self.value))
        self.value = str_to_col(self.value, is_col in df.columns)
        return evaluate_expectation(df, self.value)
