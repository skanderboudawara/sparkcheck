"""
This module contains the column expectations classes.

Dev rules:

- Suffix the class name with `Expectation` to make it clear that it is an
    expectation class.
- The class should inherit from `ColumnsExpectations` to ensure that it
    has the necessary methods.
- The class should have a `constraint` property that returns the constraint
    to apply to the DataFrame.
- The class should have a `get_message` method that formats the message
    based on the check result.
- The class should have an `eval_expectation` method that returns the
    expectation result.
- The class should have a `check_inputs` decorator to check the class input
- The class should have a `check_column_exist` decorator to check if the
    column exists in the DataFrame.
- The class should have a `validate_expectation` decorator to validate the
    expectation keys
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import Column, DataFrame

from ..constants import OPERATOR_MAP
from ..ext._decorators import (
    add_class_prefix,
    check_column_exist,
    check_dataframe,
    check_inputs,
    validate_expectation,
)
from ..ext._utils import (
    _op_check,
    _resolve_msg,
    _substitute,
    eval_first_fail,
    to_col,
    to_name,
)
from ._base import ColumnsExpectations


class ColNonNullCheck(ColumnsExpectations):
    """
    Check if a column is not null.
    """

    @check_inputs
    def __init__(
        self,
        column: str | Column,
        value: bool,
        message: str | None = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        """
        This class checks if a column is not null.

        :param column: (str | Column), the column to check
        :param value: (bool), the value to check
        :param message: (str | None), the message to display
        :return: None
        :raises: (TypeError), If the value is not of type bool
        """
        self.column = column
        self.message = message
        self.value = value

    @property
    def constraint(self) -> Column:
        """
        This method returns the constraint.

        :param: None
        :returns: None
        """
        self.column = to_col(self.column)
        return self.column.isNotNull()

    @add_class_prefix
    def get_message(self, has_failed: bool) -> None:
        """
        This method returns the message result formatted with the check.

        :param has_failed: (bool), True if the check failed, False if not
        :returns: (str), the message
        """
        default_msg = (
            f"The column `{to_name(self.column)}` <$did not|did> "
            f"meet the expectation of {type(self).__name__}"
        )
        self.message = _resolve_msg(default_msg, self.message)
        self.message = _substitute(self.message, has_failed, "<$did not|did>")

    @validate_expectation
    @check_dataframe
    @check_column_exist
    def eval_expectation(self, target: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param target: (DataFrame), the DataFrame to check
        :return: (dict), the expectation result
        """
        if not self.value:
            return ColNullCheck(
                self.column,
                True,
                self.message,
            ).eval_expectation(target)
        has_failed, count_cases, first_failed_row = eval_first_fail(
            target,
            self.column,
            self.constraint,
        )
        self.get_message(has_failed)
        return {
            "has_failed": has_failed,
            "got": count_cases,
            "message": self.message,
            "example": first_failed_row,
        }


class ColNullCheck(ColumnsExpectations):
    """
    Check if a column is null.
    """

    @check_inputs
    def __init__(
        self,
        column: str | Column,
        value: bool,
        message: str | None = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        """
        This class checks if a column is null.

        :param column: (str | Column), the column to check
        :param value: (bool), the value to check
        :param message: (str | None), the message to display
        :return: None
        :raises: (TypeError), If the value is not of type bool
        """
        self.column = column
        self.message = message
        self.value = value

    @property
    def constraint(self) -> Column:
        """
        This method returns the constraint.

        :param: None
        :returns: None
        """
        self.column = to_col(self.column)
        return self.column.isNull()

    @add_class_prefix
    def get_message(self, has_failed: bool) -> None:
        """
        This method returns the message result formatted with the check.

        :param has_failed: (bool), True if the check failed, False if not
        :returns: (str), the message
        """
        default_msg = (
            f"The column `{to_name(self.column)}` <$did not|did> "
            f"meet the expectation of {type(self).__name__}"
        )
        self.message = _resolve_msg(default_msg, self.message)
        self.message = _substitute(self.message, has_failed, "<$did not|did>")

    @validate_expectation
    @check_dataframe
    @check_column_exist
    def eval_expectation(self, target: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param target: (DataFrame), the DataFrame to check
        :return: (dict), the expectation result
        """
        if not self.value:
            return ColNonNullCheck(
                self.column,
                True,
                self.message,
            ).eval_expectation(target)
        has_failed, count_cases, first_failed_row = eval_first_fail(
            target,
            self.column,
            self.constraint,
        )
        self.get_message(has_failed)
        return {
            "has_failed": has_failed,
            "got": count_cases,
            "message": self.message,
            "example": first_failed_row,
        }


class ColRegexLikeCheck(ColumnsExpectations):
    """
    Check if a column matches a pattern.
    """

    @check_inputs
    def __init__(
        self,
        column: str | Column,
        value: str | Column,
        message: str | None = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        """
        This class checks if a column matches a pattern.

        :param column: (str | Column), the column to check
        :param value: (str), the value to match
        :param message: (str | None), the message to display
        :return: None
        :raises: (TypeError), If the value is not of type str
        """
        self.column = column
        self.message = message
        self.value = value
        self.is_spark35: bool = True
        self.is_col: bool = True

    @property
    def constraint(self) -> Column:
        """
        This method returns the constraint.

        :param: None
        :returns: None
        """
        self.column = to_col(self.column)
        if self.is_spark35:
            from pyspark.sql.functions import rlike  # noqa: PLC0415

            return rlike(self.column, self.value)
        from pyspark.sql.functions import expr  # noqa: PLC0415

        if self.is_col:
            return expr(f"{to_name(self.column)} RLIKE {to_name(self.value)}")

        return expr(f"{to_name(self.column)} RLIKE '{self.value}'")

    @add_class_prefix
    def get_message(self, has_failed: bool) -> None:
        """
        This method returns the message result formatted with the check.

        :param has_failed: (bool), True if the check failed, False if not
        :returns: (str), the message
        """
        default_msg = (
            f"The column `{to_name(self.column)}` <$did not|did> "
            f"respect the pattern `{to_name(self.value)}`"
        )
        self.message = _resolve_msg(default_msg, self.message)
        self.message = _substitute(self.message, has_failed, "<$did not|did>")

    @validate_expectation
    @check_dataframe
    @check_column_exist
    def eval_expectation(self, target: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param target: (DataFrame), the DataFrame to check
        :return: (dict), the expectation result
        """
        self.is_spark35 = target.sparkSession.version >= "3.5"
        self.is_col = self.value in target.columns
        self.value = to_col(
            self.value,
            is_col=self.is_col,
            escaped=True,
            default="lit" if self.is_spark35 else "raw",
        )

        has_failed, count_cases, first_failed_row = eval_first_fail(
            target,
            self.column,
            self.constraint,
        )
        self.get_message(has_failed)
        return {
            "has_failed": has_failed,
            "got": count_cases,
            "message": self.message,
            "example": first_failed_row,
        }


class ColIsInCheck(ColumnsExpectations):
    """
    Check if a column is in a list of values.
    """

    @check_inputs
    def __init__(
        self,
        column: str | Column,
        value: Any,
        message: str | None = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        """
        This class checks if a column is in an array.

        :param column: (str | Column), the column to check
        :param value: (Column | str | list[Column] | list[str]),
            the value to check
        :param message: (str | None), the message to display
        :return: None
        :raises: (TypeError), If the value is not of type Column | str | list
        :raises: (ValueError), If the value is empty
        """
        self.column = column
        self.message = message
        self.value = value if isinstance(value, list | tuple) else [value]

    @property
    def constraint(self) -> Column:
        """
        This method returns the constraint.

        :param: None

        :returns: None
        """
        self.column = to_col(self.column)
        return self.column.isin(*self.value)

    @add_class_prefix
    def get_message(self, has_failed: bool) -> None:
        """
        This method returns the message result formatted with the check.

        :param has_failed: (bool), True if the check failed, False if not
        :returns: (str), the message
        """
        default_msg = str(
            f"The column `{to_name(self.column)}` <$is not|is> "
            f"in `[{self.expected}]`",
        )
        self.message = _resolve_msg(default_msg, self.message)
        self.message = _substitute(self.message, has_failed, "<$is not|is>")

    def _prepare_isin_list(self, target: DataFrame) -> None:
        """
        This method prepares the list of values to check.

        :param target: (DataFrame), the DataFrame to check
        :return: None
        """
        string_values = [to_name(c) for c in self.value]
        string_values = list(set(string_values))
        string_values.sort()
        self.expected = ", ".join(string_values)
        self.value = [to_col(c, c in target.columns) for c in self.value]

    @validate_expectation
    @check_dataframe
    @check_column_exist
    def eval_expectation(self, target: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param target: (DataFrame), the DataFrame to check
        :return: (dict), the expectation result
        """
        self._prepare_isin_list(target)
        # To make NoneObject as list constraint
        target = target.select(self.column).fillna("NoneObject")
        has_failed, count_cases, first_failed_row = eval_first_fail(
            target,
            self.column,
            self.constraint,
        )
        self.get_message(has_failed)
        return {
            "has_failed": has_failed,
            "got": count_cases,
            "message": self.message,
            "example": first_failed_row,
        }


class ColCompareCheck(ColumnsExpectations):
    """
    Check if a column meets a comparison condition.
    """

    @check_inputs
    def __init__(
        self,
        column: str | Column,
        value: str | float | int | Column | bool | None,
        operator: str,
        message: str | None = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        """
        This class compares a column to a value.

        :param column: (str | Column), the column to compare
        :param value: (str | float | int | Column | bool), the value to compare
        :param operator: (str), the operator to use
        :param message: (str | None), the message to display
        :return: None
        :raises: (TypeError), If the value is not of type str | float | int
        :raises: (ValueError), If the operator is not valid.
        """
        self.column = column
        self.message = message
        _op_check(self, operator)
        self.operator = operator
        self.value = value

    @property
    def constraint(self) -> Column:
        """
        This method returns the constraint.

        :param: None
        :returns: None
        """
        self.column = to_col(self.column)
        return OPERATOR_MAP[self.operator](
            self.column,
            self.value,  # type: ignore
        )

    @add_class_prefix
    def get_message(self, has_failed: bool) -> None:
        """
        This method returns the message result formatted with the check.

        :param has_failed: (bool), True if the check failed, False if not
        :returns: (str), the message
        """
        default_message = (
            f"The column `{to_name(self.column)}` "
            f"<$is not|is> {self.operator.replace('_', ' ')} "
            f"<$to|than> `{self.expected}`"
        )
        self.message = _resolve_msg(default_message, self.message)
        self.message = _substitute(self.message, has_failed, "<$is not|is>")
        self.message = _substitute(
            self.message,
            self.operator in {"equal", "different"},
            "<$to|than>",
        )

    @validate_expectation
    @check_dataframe
    @check_column_exist
    def eval_expectation(self, target: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param target: (DataFrame), the DataFrame to check
        :return: (dict), the expectation result
        """
        self.expected = self.value
        is_col = to_name(str(self.value))
        self.value = to_col(self.value, is_col in target.columns)
        has_failed, count_cases, first_failed_row = eval_first_fail(
            target,
            self.column,
            self.constraint,
        )

        self.get_message(has_failed)

        return {
            "has_failed": has_failed,
            "got": count_cases,
            "message": self.message,
            "example": first_failed_row,
        }
