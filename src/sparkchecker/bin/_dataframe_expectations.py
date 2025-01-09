from abc import ABC, abstractmethod
from typing import Any, Union

from pyspark.sql import DataFrame
from pyspark.sql.types import DataType

from sparkchecker.bin._constants import OPERATOR_MAP
from sparkchecker.bin._decorators import check_message, validate_expectation
from sparkchecker.bin._utils import _check_operator, _override_msg, _placeholder


class DataFrameExpectation(ABC):
    @check_message
    def __init__(
        self,
        message: Union[str, None] = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        if message and not isinstance(message, str):
            raise TypeError(
                f"Argument message must be of type str but got: {type(message)} for {message!r}",
            )
        self.message = message

    @abstractmethod
    def message_result(self, check: bool) -> str: ...

    @abstractmethod
    def expectation(self, df: DataFrame) -> dict: ...


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
        """
        super().__init__(message)
        self.value = value

    def message_result(self, check: bool) -> str:
        """
        This method returns the message result formatted with the check.

        :param check: (bool), the check

        :returns: (str), the message
        """
        default_msg = "The DataFrame <$is|is not> empty"
        self.message = _override_msg(default_msg, self.message)
        self.message = _placeholder(self.message, check, "<$is|is not>")

    @validate_expectation
    def expectation(self, df: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param df: (DataFrame), the DataFrame to check

        :return: (dict), the expectation result
        """
        check = df.isEmpty()
        has_failed = self.value != check
        self.message_result(check)
        return {"has_failed": has_failed, "got": check, "message": self.message}


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
        """
        super().__init__(message)
        self.value = value

    def message_result(self, check: bool) -> str:
        """
        This method returns the message result formatted with the check.

        :param check: (bool), the check

        :returns: (str), the message
        """
        default_msg = "The DataFrame <$is|is not> not empty"
        self.message = _override_msg(default_msg, self.message)
        self.message = _placeholder(self.message, check, "<$is|is not>")

    @validate_expectation
    def expectation(self, df: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param df: (DataFrame), the DataFrame to check

        :return: (dict), the expectation result
        """
        check = not (df.isEmpty())
        has_failed = self.value != check
        self.message_result(check)
        return {"has_failed": has_failed, "got": check, "message": self.message}


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
        """
        _check_operator(operator)
        if not isinstance(value, int):
            raise TypeError(
                "Argument for DataFrame count must be of type int but got: ",
                type(value),
            )
        self.value = value
        self.operator = operator
        super().__init__(message)

    def message_result(self, check: bool) -> str:
        """
        This method returns the message result formatted with the check.

        :param check: (bool), the check

        :returns: (str), the message
        """
        default_msg = (
            f"The DataFrame has {self.result} rows, which <$is|isn't>"
            f" {self.operator} <$to|than> {self.value}"
        )
        self.message = _override_msg(default_msg, self.message)
        self.message = _placeholder(self.message, check, "<$is|is not>")
        self.message = _placeholder(
            self.message,
            self.operator in {"equal", "different"},
            "<$to|than>",
        )

    @validate_expectation
    def expectation(self, df: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param df: (DataFrame), the DataFrame to check

        :return: (dict), the expectation result
        """
        count = df.count()
        # Convert the threshold to a literal value and apply the operator
        check = OPERATOR_MAP[self.operator](count, self.value)
        self.result = count
        self.message_result(check)
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
        """
        _check_operator(operator)
        if not isinstance(value, int):
            raise TypeError(
                f"Argument for DataFrame column must be of type DataType"
                f" but got: {type(value)} for {value!r}",
            )
        self.value = value
        self.operator = operator
        super().__init__(message)

    def message_result(self, check: bool) -> str:
        """
        This method returns the message result formatted with the check.

        :param check: (bool), the check

        :returns: (str), the message
        """
        default_msg = (
            f"The DataFrame has {self.result} partitions, which "
            f"<$is|isn't> {self.operator} <$to|than> {self.value}"
        )
        self.message = _override_msg(default_msg, self.message)
        self.message = _placeholder(self.message, check, "<$is|is not>")
        self.message = _placeholder(
            self.message,
            self.operator in {"equal", "different"},
            "<$to|than>",
        )

    @validate_expectation
    def expectation(self, df: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param df: (DataFrame), the DataFrame to check

        :return: (dict), the expectation result
        """
        rdd_count = df.rdd.getNumPartitions()
        # Convert the threshold to a literal value and apply the operator
        check = OPERATOR_MAP[self.operator](rdd_count, self.value)
        self.result = rdd_count
        self.message_result(check)
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
        """
        if value and not isinstance(value, DataType):
            raise TypeError(
                "Argument for DataFrame Partitions must be of type int but got: ",
                type(value),
            )
        self.value = value
        self.column = column
        super().__init__(message)

    def message_result(self, check: bool) -> str:
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
            self.message = _override_msg(default_message, self.message)
            self.message = _placeholder(self.message, check, "<$is|is not>")
        else:
            default_message = (
                f"Column {self.column} <$does|doesn't> exist in the DataFrame"
            )
            self.message = _override_msg(default_message, self.message)
            self.message = _placeholder(self.message, check, "<$does|doesn't>")

    @validate_expectation
    def expectation(self, df: DataFrame) -> dict:
        """
        This method returns the expectation result.

        :param df: (DataFrame), the DataFrame to check

        :return: (dict), the expectation result
        """
        check_exist = self.column in df.columns
        self.message_result(check_exist)

        if not check_exist:
            return {
                "has_failed": check_exist,
                "got": ", ".join(df.columns),
                "message": self.message,
            }

        if self.value:
            data_type = df.schema[self.column].dataType
            check_type = data_type == self.value
            self.message_result(check_type)
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
