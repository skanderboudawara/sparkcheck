"""
This module contains the DataFrame expectations classes.

The classes in this module are used to check the DataFrame properties.

Dev rules:
- The classes in this module should inherit from the DataFrameExpectation
    class.
- The classes in this module should implement the abstract methods from the
    DataFrameExpectation class.
- The eval_expectation in this module should be decorated with the
    @validate_expectation decorator.
- The eval_expectation in this module should be decorated with the
    @check_dataframe decorator.
- The get_message in this module should be decorated with the @add_class_prefix
    decorator.
- The __init__ in this module should be decorated with the @check_inputs
    decorator.
"""

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.types import DataType

from ..constants import OPERATOR_MAP
from ..ext._decorators import (
    add_class_prefix,
    check_dataframe,
    check_inputs,
    validate_expectation,
)
from ..ext._utils import _op_check, _resolve_msg, _substitute
from ._base import DataFrameExpectation


class DataFrameIsEmptyCheck(DataFrameExpectation):
    @check_inputs
    def __init__(
        self,
        value: bool | None = None,
        message: str | None = None,
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
        self.value = value

    @add_class_prefix
    def get_message(self, has_failed: bool) -> None:
        """
        This method returns the message result formatted with the has_failed.

        :param has_failed: (bool), the has_failed

        :returns: (str), the message
        """
        default_msg = "The DataFrame <$is not|is> empty"
        self.message = _resolve_msg(default_msg, self.message)
        self.message = _substitute(self.message, has_failed, "<$is not|is>")

    @validate_expectation
    @check_dataframe
    def eval_expectation(self, target: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param target: (DataFrame), the DataFrame to check
        :return: (dict), the expectation result
        :raises: (TypeError), If the target is not a DataFrame.
        """
        has_failed = target.isEmpty()
        has_failed = has_failed != self.value
        self.get_message(has_failed if self.value else not has_failed)
        return {
            "has_failed": has_failed,
            "got": has_failed,
            "message": self.message,
        }


class DataFrameCountThresholdCheck(DataFrameExpectation):
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

    @add_class_prefix
    def get_message(self, has_failed: bool) -> None:
        """
        This method returns the message result formatted with the has_failed.

        :param has_failed: (bool), the has_failed
        :returns: (str), the message
        """
        default_msg = (
            f"The DataFrame has {self.result} rows, which <$is not|is>"
            f" {self.operator} <$to|than> {self.value}"
        )
        self.message = _resolve_msg(default_msg, self.message)
        self.message = _substitute(self.message, has_failed, "<$is not|is>")
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
        count = target.count()
        # Convert the threshold to a literal value and apply the operator
        has_failed = not (OPERATOR_MAP[self.operator](count, self.value))
        self.result = count
        self.get_message(has_failed)
        return {
            "has_failed": has_failed,
            "got": count,
            "message": self.message,
        }


class DataFramePartitionsCountCheck(DataFrameExpectation):
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

    @add_class_prefix
    def get_message(self, has_failed: bool) -> None:
        """
        This method returns the message result formatted with the has_failed.

        :param has_failed: (bool), the has_failed
        :returns: (str), the message
        """
        default_msg = (
            f"The DataFrame has {self.result} partitions, which "
            f"<$is not|is> {self.operator} <$to|than> {self.value}"
        )
        self.message = _resolve_msg(default_msg, self.message)
        self.message = _substitute(self.message, has_failed, "<$is not|is>")
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
        rdd_count = target.rdd.getNumPartitions()
        # Convert the threshold to a literal value and apply the operator
        has_failed = not (OPERATOR_MAP[self.operator](rdd_count, self.value))
        self.result = rdd_count
        self.get_message(has_failed)
        return {
            "has_failed": has_failed,
            "got": rdd_count,
            "message": self.message,
        }


class DataFrameHasColumnsCheck(DataFrameExpectation):
    @check_inputs
    def __init__(
        self,
        column: str,
        value: DataType | None = None,
        message: str | None = None,
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

    @add_class_prefix
    def get_message(self, has_failed: bool) -> None:
        """
        This method returns the message result formatted with the has_failed.

        :param has_failed: (bool), the has_failed
        :returns: (str), the message
        """
        default_message = (
            f"Column '{self.column}' exists in the DataFrame "
            f"<$but|and> it'<$s not|s> of type: {self.value}"
            if self.value
            else f"Column '{self.column}' <$doesn't|does> "
            "exist in the DataFrame"
        )

        self.message = _resolve_msg(default_message, self.message)
        placeholders = (
            ["<$but|and>", "<$s not|s>"] if self.value else ["<$doesn't|does>"]
        )

        for placeholder in placeholders:
            self.message = _substitute(self.message, has_failed, placeholder)

    @validate_expectation
    @check_dataframe
    def eval_expectation(self, target: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param target: (DataFrame), the DataFrame to check
        :return: (dict), the expectation result
        :raises: (TypeError), If the target is not a DataFrame.
        """
        # Check if the column exists
        has_failed = self.column not in target.columns

        if has_failed:
            self.value = None  # Overwrite value if column doesn't exist
            self.get_message(True)
            return {
                "has_failed": True,
                "got": ", ".join(target.columns),
                "message": self.message,
            }

        # Check if the column type matches the expected value
        data_type = target.schema[self.column].dataType
        has_failed = data_type != self.value if self.value else has_failed

        self.get_message(has_failed)
        return {
            "has_failed": has_failed,
            "got": data_type if self.value else self.column,
            "message": self.message,
        }
