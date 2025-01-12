from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.types import DataType

from ..constants import OPERATOR_MAP
from ..ext._decorators import (
    check_dataframe,
    check_inputs,
    validate_expectation,
)
from ..ext._utils import _op_check, _resolve_msg, _substitute
from ._base import DataFrameExpectation


class DataFrameIsEmptyExpectation(DataFrameExpectation):
    @check_inputs
    def __init__(
        self,
        message: str | None = None,
        value: bool | None = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        """
        This class checks if a DataFrame is empty.

        :param message: (str), the message to display
        :param value: (bool), the value to check
        :return: None
        :raises: (TypeError), If the value is not a boolean.
        """
        self.message = message
        if not value:
            value = True
        self.value = value

    def get_message(self, check: bool) -> None:
        """
        This method returns the message result formatted with the check.

        :param check: (bool), the check

        :returns: (str), the message
        """
        default_msg = "The DataFrame <$is|is not> empty"
        self.message = _resolve_msg(default_msg, self.message)
        self.message = _substitute(self.message, check, "<$is|is not>")

    @validate_expectation
    @check_dataframe
    def eval_expectation(self, target: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param target: (DataFrame), the DataFrame to check
        :return: (dict), the expectation result
        :raises: (TypeError), If the target is not a DataFrame.
        """
        if not isinstance(target, DataFrame):
            raise TypeError(
                "Argument for DataFrame isEmpty must be of "
                "type DataFrame but got: ",
                type(target),
            )
        check = target.isEmpty()
        has_failed = self.value != check
        self.get_message(check)
        return {
            "has_failed": has_failed,
            "got": check,
            "message": self.message,
        }


class DataFrameIsNotEmptyExpectation(DataFrameExpectation):
    @check_inputs
    def __init__(
        self,
        message: str | None = None,
        value: bool | None = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        """
        This class checks if a DataFrame is empty.

        :param message: (str), the message to display
        :param value: (bool), the value to check
        :return: None
        :raises: (TypeError), If the value is not a boolean.
        """
        self.message = message
        if not value:
            value = True
        self.value = value

    def get_message(self, check: bool) -> None:
        """
        This method returns the message result formatted with the check.

        :param check: (bool), the check
        :returns: (str), the message
        """
        default_msg = "The DataFrame <$is|is not> not empty"
        self.message = _resolve_msg(default_msg, self.message)
        self.message = _substitute(self.message, check, "<$is|is not>")

    @validate_expectation
    @check_dataframe
    def eval_expectation(self, target: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param target: (DataFrame), the DataFrame to check
        :return: (dict), the expectation result
        :raises: (TypeError), If the target is not a DataFrame.
        """
        if not isinstance(target, DataFrame):
            raise TypeError(
                "Argument for DataFrame isEmpty must be of "
                "type DataFrame but got: ",
                type(target),
            )
        check = not (target.isEmpty())
        has_failed = self.value != check
        self.get_message(check)
        return {
            "has_failed": has_failed,
            "got": check,
            "message": self.message,
        }


class DataFrameCountThresholdExpectation(DataFrameExpectation):
    @check_inputs
    def __init__(
        self,
        value: int,
        operator: str,
        message: str | None = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        """
        This class compares the count of a DataFrame to a value.

        :param value: (int), the value to check
        :param operator: (str), the operator to use
        :param message: (str), the message to display
        :return: None
        :raises: (TypeError), If the value is not an integer.
        :raises: (ValueError), If the operator is not valid.
        """
        self.message = message
        _op_check(self, operator)
        self.value = value
        self.operator = operator

    def get_message(self, check: bool) -> None:
        """
        This method returns the message result formatted with the check.

        :param check: (bool), the check
        :returns: (str), the message
        """
        default_msg = (
            f"The DataFrame has {self.result} rows, which <$is|isn't>"
            f" {self.operator} <$to|than> {self.value}"
        )
        self.message = _resolve_msg(default_msg, self.message)
        self.message = _substitute(self.message, check, "<$is|is not>")
        self.message = _substitute(
            self.message,
            self.operator in {"equal", "different"},
            "<$to|than>",
        )

    @validate_expectation
    @check_dataframe
    def eval_expectation(self, target: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param target: (DataFrame), the DataFrame to check
        :return: (dict), the expectation result
        :raises: (TypeError), If the target is not a DataFrame.
        """
        if not isinstance(target, DataFrame):
            raise TypeError(
                "Argument for DataFrame isEmpty must be of "
                "type DataFrame but got: ",
                type(target),
            )
        count = target.count()
        # Convert the threshold to a literal value and apply the operator
        check = OPERATOR_MAP[self.operator](count, self.value)
        self.result = count
        self.get_message(check)
        return {
            "has_failed": not (check),
            "got": count,
            "message": self.message,
        }


class DataFramePartitionsCountExpectation(DataFrameExpectation):
    @check_inputs
    def __init__(
        self,
        value: int,
        operator: str,
        message: str | None = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        """
        This class compares the number of partitions of a DataFrame to a value.

        :param value: (int), the value to check
        :param operator: (str), the operator to use
        :param message: (str), the message to display
        :return: None
        :raises: (TypeError), If the value is not an integer.
        :raises: (ValueError), If the operator is not valid.
        """
        self.message = message
        _op_check(self, operator)
        self.value = value
        self.operator = operator

    def get_message(self, check: bool) -> None:
        """
        This method returns the message result formatted with the check.

        :param check: (bool), the check
        :returns: (str), the message
        """
        default_msg = (
            f"The DataFrame has {self.result} partitions, which "
            f"<$is|isn't> {self.operator} <$to|than> {self.value}"
        )
        self.message = _resolve_msg(default_msg, self.message)
        self.message = _substitute(self.message, check, "<$is|is not>")
        self.message = _substitute(
            self.message,
            self.operator in {"equal", "different"},
            "<$to|than>",
        )

    @validate_expectation
    @check_dataframe
    def eval_expectation(self, target: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param target: (DataFrame), the DataFrame to check
        :return: (dict), the expectation result
        :raises: (TypeError), If the target is not a DataFrame.
        """
        if not isinstance(target, DataFrame):
            raise TypeError(
                "Argument for DataFrame isEmpty must be of "
                "type DataFrame but got: ",
                type(target),
            )
        rdd_count = target.rdd.getNumPartitions()
        # Convert the threshold to a literal value and apply the operator
        check = OPERATOR_MAP[self.operator](rdd_count, self.value)
        self.result = rdd_count
        self.get_message(check)
        return {
            "has_failed": not (check),
            "got": rdd_count,
            "message": self.message,
        }


class DataFrameHasColumnsExpectation(DataFrameExpectation):
    @check_inputs
    def __init__(
        self,
        column: str,
        message: str | None = None,
        value: DataType | None = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        """
        This class checks if a column exists in a DataFrame.

        :param column: (str), the column to check
        :param value: (DataType), the value to check
        :param message: (str), the message to display
        :return: None
        :raises: (TypeError), If the value is not a DataType.
        """
        self.message = message
        self.value = value
        self.column = column

    def get_message(self, check: bool) -> None:
        """
        This method returns the message result formatted with the check.

        :param check: (bool), the check
        :returns: (str), the message
        """
        if self.value:
            default_message = (
                f"Column {self.column} exists in the DataFrame"
                f"and <$is|is not> of type: {self.value}"
            )
            self.message = _resolve_msg(default_message, self.message)
            self.message = _substitute(self.message, check, "<$is|is not>")
        else:
            default_message = (
                f"Column {self.column} <$does|doesn't> exist in the DataFrame"
            )
            self.message = _resolve_msg(default_message, self.message)
            self.message = _substitute(self.message, check, "<$does|doesn't>")

    @validate_expectation
    @check_dataframe
    def eval_expectation(self, target: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param target: (DataFrame), the DataFrame to check
        :return: (dict), the expectation result
        :raises: (TypeError), If the target is not a DataFrame.
        """
        if not isinstance(target, DataFrame):
            raise TypeError(
                "Argument for DataFrame isEmpty must be of "
                "type DataFrame but got: ",
                type(target),
            )
        check_exist = self.column in target.columns
        self.get_message(check_exist)

        if not check_exist:
            return {
                "has_failed": check_exist,
                "got": ", ".join(target.columns),
                "message": self.message,
            }

        if self.value:
            data_type = target.schema[self.column].dataType
            check_type = data_type == self.value
            self.get_message(check_type)
            return {
                "has_failed": not (check_type),
                "got": data_type,
                "message": self.message,
            }

        return {
            "has_failed": not (check_exist),
            "got": self.column,
            "message": self.message,
        }
