from abc import ABC, abstractmethod
from typing import Any, Union

from pyspark.sql import Column, DataFrame

from sparkchecker.bin._constants import OPERATOR_MAP
from sparkchecker.bin._decorators import (
    check_column_exist,
    check_message,
    validate_expectation,
)
from sparkchecker.bin._utils import (
    _check_operator,
    _override_msg,
    _placeholder,
    args_to_list_cols,
    col_to_name,
    str_to_col,
)


def evaluate_expectation(
    column: Union[str, Column],
    df: DataFrame,
    expectation: Column,
) -> tuple[bool, int, dict]:
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
    column = str_to_col(column)
    # We need to check the opposite of our expectations
    df = df.select(column).filter(~expectation)
    first_failed_row = df.first()
    check = bool(not first_failed_row)
    count_cases = df.count() if check else 0
    return check, count_cases, first_failed_row.asDict() if first_failed_row else {}


class ColumnsExpectations(ABC):
    @check_message
    def __init__(
        self,
        col_name: Union[str, Column],
        message: Union[str, None] = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        self.column = col_name
        self.message = message

    @property
    @abstractmethod
    def constraint(self) -> Column: ...

    @abstractmethod
    @validate_expectation
    @check_column_exist
    def expectation(self, df: DataFrame) -> dict: ...

    @abstractmethod
    def message_result(self, check: bool) -> str: ...


class NonNullColumn(ColumnsExpectations):

    @check_message
    def __init__(
        self,
        column: Union[str, Column],
        value: bool,
        message: Union[str, None] = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        """
        This class checks if a column is not null.

        :param column: (Union[str, Column]), the column to check

        :param value: (bool), the value to check

        :param message: (Union[str, None]), the message to display

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
        """
        This method returns the constraint.

        :param: None

        :returns: None
        """
        self.column = str_to_col(self.column)
        return self.column.isNotNull()

    def message_result(self, check: bool) -> str:
        """
        This method returns the message result formatted with the check.

        :param check: (bool), the check

        :returns: (str), the message
        """
        default_msg = f"The column {col_to_name(self.column)} <$is|is not> Not Null"
        self.message = _override_msg(default_msg, self.message)
        self.message = _placeholder(self.message, check, "<$is|is not>")

    @validate_expectation
    @check_column_exist
    def expectation(self, df: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param df: (DataFrame), the DataFrame to check

        :return: (dict), the expectation result
        """
        check, count_cases, first_failed_row = evaluate_expectation(
            self.column,
            df,
            self.constraint,
        )
        has_failed = self.value != check
        self.message_result(check)
        return {
            "has_failed": has_failed,
            "got": count_cases,
            "message": self.message,
            "example": first_failed_row,
        }


class NullColumn(ColumnsExpectations):
    @check_message
    def __init__(
        self,
        column: Union[str, Column],
        value: bool,
        message: Union[str, None] = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        """
        This class checks if a column is null.

        :param column: (Union[str, Column]), the column to check

        :param value: (bool), the value to check

        :param message: (Union[str, None]), the message to display

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
        """
        This method returns the constraint.

        :param: None

        :returns: None
        """
        self.column = str_to_col(self.column)
        return self.column.isNull()

    def message_result(self, check: bool) -> str:
        """
        This method returns the message result formatted with the check.

        :param check: (bool), the check

        :returns: (str), the message
        """
        default_msg = f"The column {col_to_name(self.column)} <$is not|is> Null"
        self.message = _override_msg(default_msg, self.message)
        self.message = _placeholder(self.message, check, "<$is not|is>")

    @validate_expectation
    @check_column_exist
    def expectation(self, df: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param df: (DataFrame), the DataFrame to check

        :return: (dict), the expectation result
        """
        check, count_cases, first_failed_row = evaluate_expectation(
            self.column,
            df,
            self.constraint,
        )
        has_failed = self.value != check
        self.message_result(check)
        return {
            "has_failed": has_failed,
            "got": count_cases,
            "message": self.message,
            "example": first_failed_row,
        }


class RlikeColumn(ColumnsExpectations):
    @check_message
    def __init__(
        self,
        column: Union[str, Column],
        value: str,
        message: Union[str, None] = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        """
        This class checks if a column matches a pattern.

        :param column: (Union[str, Column]), the column to check

        :param value: (str), the value to match

        :param message: (Union[str, None]), the message to display

        :return: None
        """
        super().__init__(column, message)
        if not isinstance(value, str):
            raise TypeError(
                "Argument pattern `value` must be of type bool but got: ",
                type(value),
            )
        self.value = rf"{value}"

    @property
    def constraint(self) -> Column:
        """
        This method returns the constraint.

        :param: None

        :returns: None
        """
        self.column = str_to_col(self.column)
        return self.column.rlike(self.value)

    def message_result(self, check: bool) -> str:
        """
        This method returns the message result formatted with the check.

        :param check: (bool), the check

        :returns: (str), the message
        """
        default_msg = (
            f"The column {col_to_name(self.column)} <$does|doesn't>"
            f" respect the pattern `{self.value}`"
        )
        self.message = _override_msg(default_msg, self.message)
        self.message = _placeholder(self.message, check, "<$does|doesn't>")

    @validate_expectation
    @check_column_exist
    def expectation(self, df: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param df: (DataFrame), the DataFrame to check

        :return: (dict), the expectation result
        """
        check, count_cases, first_failed_row = evaluate_expectation(
            self.column,
            df,
            self.constraint,
        )
        self.message_result(check)
        return {
            "has_failed": not (check),
            "got": count_cases,
            "message": self.message,
            "example": first_failed_row,
        }


class IsInColumn(ColumnsExpectations):
    @check_message
    def __init__(
        self,
        column: Union[str, Column],
        value: Union[Column, str, list[Column], list[str]],
        message: Union[str, None] = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        """
        This class checks if a column is in an array.

        :param column: (Union[str, Column]), the column to check

        :param value: (Union[Column, str, list[Column], list[str]]), the value to check

        :param message: (Union[str, None]), the message to display

        :return: None
        """
        super().__init__(column, message)
        if not isinstance(value, (str, Column, list)):
            raise TypeError(
                "Argument for in `value` must be of type \
                    Union[Column, str, list[Column], list[str]] but got: ",
                type(value),
            )
        if not value:
            raise ValueError("Argument for in `value` must not be empty")
        self.value = value

    @property
    def constraint(self) -> Column:
        """
        This method returns the constraint.

        :param: None

        :returns: None
        """
        self.value = args_to_list_cols(self.value, is_col=False)
        self.column = str_to_col(self.column)
        return self.column.isin(*self.value)

    def message_result(self, check: bool) -> str:
        """
        This method returns the message result formatted with the check.

        :param check: (bool), the check

        :returns: (str), the message
        """
        default_msg = (
            f"The column {col_to_name(self.column)} <$is|is not> in `{self.expected}`"
        )
        self.message = _override_msg(default_msg, self.message)
        self.message = _placeholder(self.message, check, "<$is|is not>")

    @validate_expectation
    @check_column_exist
    def expectation(self, df: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param df: (DataFrame), the DataFrame to check

        :return: (dict), the expectation result
        """
        check, count_cases, first_failed_row = evaluate_expectation(
            self.column,
            df,
            self.constraint,
        )
        self.expected = ", ".join([col_to_name(c) for c in self.value])
        self.message_result(check)
        return {
            "has_failed": not (check),
            "got": count_cases,
            "message": self.message,
            "example": first_failed_row,
        }


class ColumnCompare(ColumnsExpectations):
    @check_message
    def __init__(
        self,
        column: Union[str, Column],
        value: Union[str, float, Column],
        operator: str,
        message: Union[str, None] = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        """
        This class compares a column to a value.

        :param column: (Union[str, Column]), the column to compare

        :param value: (Union[str, float]), the value to compare

        :param operator: (str), the operator to use

        :param message: (Union[str, None]), the message to display

        :return: None
        """
        super().__init__(column, message)
        _check_operator(operator)
        self.operator = operator
        if not isinstance(value, (str, float, int, Column)):
            raise TypeError(
                "Argument for column comparison `value` must be of type \
                    Union[str, float] but got: ",
                type(value),
            )
        self.value = value

    @property
    def constraint(self) -> Column:
        """
        This method returns the constraint.

        :param: None

        :returns: None
        """
        self.column = str_to_col(self.column)
        return OPERATOR_MAP[self.operator](self.column, self.value)

    def message_result(self, check: bool) -> str:
        """
        This method returns the message result formatted with the check.

        :param check: (bool), the check

        :returns: (str), the message
        """
        default_message = (
            f"The column {col_to_name(self.column)} "
            f"<$is|is not> {self.operator} <$to|than> `{self.expected}`"
        )
        self.message = _override_msg(default_message, self.message)
        self.message = _placeholder(self.message, check, "<$is|is not>")
        self.message = _placeholder(
            self.message,
            self.operator in {"equal", "different"},
            "<$to|than>",
        )

    @validate_expectation
    @check_column_exist
    def expectation(self, df: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param df: (DataFrame), the DataFrame to check

        :return: (dict), the expectation result
        """
        self.expected = self.value
        is_col = col_to_name(str(self.value))
        self.value = str_to_col(self.value, is_col in df.columns)

        check, count_cases, first_failed_row = evaluate_expectation(
            self.column,
            df,
            self.constraint,
        )

        self.message_result(check)

        return {
            "has_failed": not (check),
            "got": count_cases,
            "message": self.message,
            "example": first_failed_row,
        }
