from typing import Any, Union

from pyspark.sql import DataFrame
from pyspark.sql.types import DataType

from ..constants import OPERATOR_MAP
from ..ext._decorators import check_message, validate_expectation
from ..ext._utils import _check_operator, _resolve_msg, _substitute
from ._base import DataFrameExpectation


class IsEmpty(DataFrameExpectation):
    @check_message
    def __init__(
        self,
        message: Union[str, None] = None,
        value: Union[bool, None] = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        """
        This class checks if a DataFrame is empty.

        :param message: (str), the message to display
        :param value: (bool), the value to check
        :return: None
        :raises: (TypeError), If the value is not a boolean.
        """
        super().__init__(message)
        if not value:
            value = True
        if value and not isinstance(value, bool):
            raise TypeError(
                "Argument for DataFrame isEmpty must be of type bool but got: ",
                type(value),
            )
        self.value = value

    def get_message(self, check: bool) -> str:
        """
        This method returns the message result formatted with the check.

        :param check: (bool), the check

        :returns: (str), the message
        """
        default_msg = "The DataFrame <$is|is not> empty"
        self.message = _resolve_msg(default_msg, self.message)
        self.message = _substitute(self.message, check, "<$is|is not>")

    @validate_expectation
    def eval_expectation(self, target: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param target: (DataFrame), the DataFrame to check
        :return: (dict), the expectation result
        :raises: (TypeError), If the target is not a DataFrame.
        """
        if not isinstance(target, DataFrame):
            raise TypeError(
                "Argument for DataFrame isEmpty must be of type DataFrame but got: ",
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


class IsNotEmpty(DataFrameExpectation):
    @check_message
    def __init__(
        self,
        message: Union[str, None] = None,
        value: Union[bool, None] = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        """
        This class checks if a DataFrame is empty.

        :param message: (str), the message to display
        :param value: (bool), the value to check
        :return: None
        :raises: (TypeError), If the value is not a boolean.
        """
        super().__init__(message)
        if not value:
            value = True
        if value and not isinstance(value, bool):
            raise TypeError(
                "Argument for DataFrame isEmpty must be of type bool but got: ",
                type(value),
            )
        self.value = value

    def get_message(self, check: bool) -> str:
        """
        This method returns the message result formatted with the check.

        :param check: (bool), the check
        :returns: (str), the message
        """
        default_msg = "The DataFrame <$is|is not> not empty"
        self.message = _resolve_msg(default_msg, self.message)
        self.message = _substitute(self.message, check, "<$is|is not>")

    @validate_expectation
    def eval_expectation(self, target: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param target: (DataFrame), the DataFrame to check
        :return: (dict), the expectation result
        :raises: (TypeError), If the target is not a DataFrame.
        """
        if not isinstance(target, DataFrame):
            raise TypeError(
                "Argument for DataFrame isEmpty must be of type DataFrame but got: ",
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


class CountThreshold(DataFrameExpectation):
    @check_message
    def __init__(
        self,
        value: int,
        operator: str,
        message: Union[str, None] = None,
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
        super().__init__(message)
        _check_operator(operator)
        if not isinstance(value, int):
            raise TypeError(
                "Argument for DataFrame count must be of type int but got: ",
                type(value),
            )
        self.value = value
        self.operator = operator

    def get_message(self, check: bool) -> str:
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
    def eval_expectation(self, target: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param target: (DataFrame), the DataFrame to check
        :return: (dict), the expectation result
        :raises: (TypeError), If the target is not a DataFrame.
        """
        if not isinstance(target, DataFrame):
            raise TypeError(
                "Argument for DataFrame isEmpty must be of type DataFrame but got: ",
                type(target),
            )
        count = target.count()
        # Convert the threshold to a literal value and apply the operator
        check = OPERATOR_MAP[self.operator](count, self.value)
        self.result = count
        self.get_message(check)
        return {"has_failed": not (check), "got": count, "message": self.message}


class PartitionsCount(DataFrameExpectation):
    @check_message
    def __init__(
        self,
        value: int,
        operator: str,
        message: Union[str, None] = None,
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
        super().__init__(message)
        _check_operator(operator)
        if not isinstance(value, int):
            raise TypeError(
                f"Argument for DataFrame column must be of type DataType"
                f" but got: {type(value)} for {value!r}",
            )
        self.value = value
        self.operator = operator

    def get_message(self, check: bool) -> str:
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
    def eval_expectation(self, target: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param target: (DataFrame), the DataFrame to check
        :return: (dict), the expectation result
        :raises: (TypeError), If the target is not a DataFrame.
        """
        if not isinstance(target, DataFrame):
            raise TypeError(
                "Argument for DataFrame isEmpty must be of type DataFrame but got: ",
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


class Exist(DataFrameExpectation):
    @check_message
    def __init__(
        self,
        column: str,
        message: Union[str, None] = None,
        value: Union[DataType, None] = None,
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
        super().__init__(message)
        if value and not isinstance(value, DataType):
            raise TypeError(
                "Argument for DataFrame Partitions must be of type int but got: ",
                type(value),
            )
        self.value = value
        self.column = column

    def get_message(self, check: bool) -> str:
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
    def eval_expectation(self, target: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param target: (DataFrame), the DataFrame to check
        :return: (dict), the expectation result
        :raises: (TypeError), If the target is not a DataFrame.
        """
        if not isinstance(target, DataFrame):
            raise TypeError(
                "Argument for DataFrame isEmpty must be of type DataFrame but got: ",
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