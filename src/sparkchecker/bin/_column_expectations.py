"""
This module contains the column expectations classes.
"""

from typing import Any

from pyspark.sql import Column, DataFrame

from ..constants import OPERATOR_MAP
from ..ext._decorators import (
    check_column_exist,
    check_message,
    validate_expectation,
)
from ..ext._utils import (
    _check_operator,
    _resolve_msg,
    _substitute,
    args_to_list_cols,
    col_to_name,
    evaluate_first_fail,
    str_to_col,
)
from ._base import ColumnsExpectations


class NonNullColumn(ColumnsExpectations):

    @check_message
    def __init__(
        self,
        column: str | Column,
        value: bool,
        message: str | None = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        """
        This class checks if a column is not null.

        :param column: (Union[str, Column]), the column to check
        :param value: (bool), the value to check
        :param message: (Union[str, None]), the message to display
        :return: None
        :raises: (TypeError), If the value is not of type bool
        """
        self.column = column
        self.message = message
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

    def get_message(self, check: bool) -> None:
        """
        This method returns the message result formatted with the check.

        :param check: (bool), the check
        :returns: (str), the message
        """
        default_msg = f"The column {col_to_name(self.column)} <$is|is not> Not Null"
        self.message = _resolve_msg(default_msg, self.message)
        self.message = _substitute(self.message, check, "<$is|is not>")

    @validate_expectation
    @check_column_exist
    def eval_expectation(self, target: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param target: (DataFrame), the DataFrame to check
        :return: (dict), the expectation result
        """
        check, count_cases, first_failed_row = evaluate_first_fail(
            target,
            self.column,
            self.constraint,
        )
        has_failed = self.value != check
        self.get_message(check)
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
        column: str | Column,
        value: bool,
        message: str | None = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        """
        This class checks if a column is null.

        :param column: (Union[str, Column]), the column to check
        :param value: (bool), the value to check
        :param message: (Union[str, None]), the message to display
        :return: None
        :raises: (TypeError), If the value is not of type bool
        """
        self.column = column
        self.message = message
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

    def get_message(self, check: bool) -> None:
        """
        This method returns the message result formatted with the check.

        :param check: (bool), the check
        :returns: (str), the message
        """
        default_msg = f"The column {col_to_name(self.column)} <$is not|is> Null"
        self.message = _resolve_msg(default_msg, self.message)
        self.message = _substitute(self.message, check, "<$is not|is>")

    @validate_expectation
    @check_column_exist
    def eval_expectation(self, target: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param target: (DataFrame), the DataFrame to check
        :return: (dict), the expectation result
        """
        check, count_cases, first_failed_row = evaluate_first_fail(
            target,
            self.column,
            self.constraint,
        )
        has_failed = self.value != check
        self.get_message(check)
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
        column: str | Column,
        value: str,
        message: str | None = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        """
        This class checks if a column matches a pattern.

        :param column: (Union[str, Column]), the column to check
        :param value: (str), the value to match
        :param message: (Union[str, None]), the message to display
        :return: None
        :raises: (TypeError), If the value is not of type str
        """
        self.column = column
        self.message = message
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

    def get_message(self, check: bool) -> None:
        """
        This method returns the message result formatted with the check.

        :param check: (bool), the check
        :returns: (str), the message
        """
        default_msg = (
            f"The column {col_to_name(self.column)} <$does|doesn't>"
            f" respect the pattern `{self.value}`"
        )
        self.message = _resolve_msg(default_msg, self.message)
        self.message = _substitute(self.message, check, "<$does|doesn't>")

    @validate_expectation
    @check_column_exist
    def eval_expectation(self, target: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param target: (DataFrame), the DataFrame to check
        :return: (dict), the expectation result
        """
        check, count_cases, first_failed_row = evaluate_first_fail(
            target,
            self.column,
            self.constraint,
        )
        self.get_message(check)
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
        column: str | Column,
        value: Column | str | list[Column] | list[str],
        message: str | None = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        """
        This class checks if a column is in an array.

        :param column: (Union[str, Column]), the column to check
        :param value: (Union[Column, str, list[Column], list[str]]), the value to check
        :param message: (Union[str, None]), the message to display
        :return: None
        :raises: (TypeError), If the value is not of type Union[Column, str, list]
        :raises: (ValueError), If the value is empty
        """
        self.column = column
        self.message = message
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
        if not isinstance(self.value, float | str | Column | list | tuple):
            raise TypeError(
                "Argument for in `value` must be of type \
                    Union[Column, str, list[Column], list[str]] but got: ",
                type(self.value),
            )
        self.value = args_to_list_cols(self.value, is_col=False)
        self.column = str_to_col(self.column)
        return self.column.isin(*self.value)

    def get_message(self, check: bool) -> None:
        """
        This method returns the message result formatted with the check.

        :param check: (bool), the check
        :returns: (str), the message
        """
        default_msg = (
            f"The column {col_to_name(self.column)} <$is|is not> in `{self.expected}`"
        )
        self.message = _resolve_msg(default_msg, self.message)
        self.message = _substitute(self.message, check, "<$is|is not>")

    @validate_expectation
    @check_column_exist
    def eval_expectation(self, target: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param target: (DataFrame), the DataFrame to check
        :return: (dict), the expectation result
        """
        self.expected = ", ".join([col_to_name(c) for c in self.value])
        check, count_cases, first_failed_row = evaluate_first_fail(
            target,
            self.column,
            self.constraint,
        )
        self.get_message(check)
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
        column: str | Column,
        value: str | float | Column,
        operator: str,
        message: str | None = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        """
        This class compares a column to a value.

        :param column: (Union[str, Column]), the column to compare
        :param value: (Union[str, float]), the value to compare
        :param operator: (str), the operator to use
        :param message: (Union[str, None]), the message to display
        :return: None
        :raises: (TypeError), If the value is not of type Union[str, float]
        :raises: (ValueError), If the operator is not valid.
        """
        self.column = column
        self.message = message
        _check_operator(operator)
        self.operator = operator
        if not isinstance(value, str | float | int | Column):
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

    def get_message(self, check: bool) -> None:
        """
        This method returns the message result formatted with the check.

        :param check: (bool), the check
        :returns: (str), the message
        """
        default_message = (
            f"The column {col_to_name(self.column)} "
            f"<$is|is not> {self.operator} <$to|than> `{self.expected}`"
        )
        self.message = _resolve_msg(default_message, self.message)
        self.message = _substitute(self.message, check, "<$is|is not>")
        self.message = _substitute(
            self.message,
            self.operator in {"equal", "different"},
            "<$to|than>",
        )

    @validate_expectation
    @check_column_exist
    def eval_expectation(self, target: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param target: (DataFrame), the DataFrame to check
        :return: (dict), the expectation result
        """
        self.expected = self.value
        is_col = col_to_name(str(self.value))
        self.value = str_to_col(self.value, is_col in target.columns)

        check, count_cases, first_failed_row = evaluate_first_fail(
            target,
            self.column,
            self.constraint,
        )

        self.get_message(check)

        return {
            "has_failed": not (check),
            "got": count_cases,
            "message": self.message,
            "example": first_failed_row,
        }
