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
                "Argument message must be of type str but got: ",
                type(message),
                f"for {message!r}",
            )
        self.message = message

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

        :param df: (DataFrame), the DataFrame to check

        :return: None
        """
        super().__init__(message)
        self.value = value

    @validate_expectation
    def expectation(self, df: DataFrame) -> dict:
        """
        This method returns the expectation.

        :param None:

        :return: (bool), the expectation
        """
        check = df.isEmpty()
        has_failed = self.value != check
        self.message = _placeholder(
            _override_msg("The DataFrame <$is_or_not> empty", self.message),
            check,
            "<$is_or_not>",
            ("is", "isn't"),
        )
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

        :param df: (DataFrame), the DataFrame to check

        :return: None
        """
        super().__init__(message)
        self.value = value

    @validate_expectation
    def expectation(self, df: DataFrame) -> dict:
        """
        This method returns the expectation.

        :param None:

        :return: (bool), the expectation
        """
        check = not (df.isEmpty())
        has_failed = self.value != check
        self.message = _placeholder(
            _override_msg("The DataFrame  <$is_or_not> empty", self.message),
            check,
            "<$is_or_not>",
            ("isn't", "is"),
        )
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

        :param df: (DataFrame), the DataFrame to check

        :param value: (int), the value to check

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

    @validate_expectation
    def expectation(self, df: DataFrame) -> dict:
        """
        This method returns the expectation.

        :param None:

        :return: (bool), the expectation
        """
        count = df.count()
        # Convert the threshold to a literal value and apply the operator
        check = OPERATOR_MAP[self.operator](count, self.value)
        self.message = _placeholder(
            _override_msg(
                f"The DataFrame has {count} rows, which <$is_or_not> {self.operator} <$to_or_than> {self.value}",  # noqa: E501
                self.message,
            ),
            check,
            "<$is_or_not>",
            ("is", "isn't"),
        )
        self.message = _placeholder(
            self.message,
            self.operator in {"equal", "different"},
            "<$to_or_than>",
            ("to", "than"),
        )
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

        :param df: (DataFrame), the DataFrame to check

        :param value: (int), the value to check

        :return: None
        """
        _check_operator(operator)
        if not isinstance(value, int):
            raise TypeError(
                "Argument for DataFrame Partitions must be of type int but got: ",
                type(value),
            )
        self.value = value
        self.operator = operator
        super().__init__(message)

    @validate_expectation
    def expectation(self, df: DataFrame) -> dict:
        """
        This method returns the expectation.

        :param None:

        :return: (bool), the expectation
        """
        rdd_count = df.rdd.getNumPartitions()
        # Convert the threshold to a literal value and apply the operator
        check_count = OPERATOR_MAP[self.operator](rdd_count, self.value)
        self.message = _placeholder(
            _override_msg(
                f"The DataFrame has {rdd_count} partitions, which <$is_or_not> {self.operator} <$to_or_than> {self.value}",  # noqa: E501
                self.message,
            ),
            check_count,
            "<$is_or_not>",
            ("is", "isn't"),
        )
        self.message = _placeholder(
            self.message,
            self.operator in {"equal", "different"},
            "<$to_or_than>",
            ("to", "than"),
        )
        return {"has_failed": not(check_count), "got": rdd_count, "message": self.message}


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

        :param df: (DataFrame), the DataFrame to check

        :param column: (str), the column to check

        :param value: (DataType), the value to check

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

    @validate_expectation
    def expectation(self, df: DataFrame) -> dict:
        """
        This method returns the expectation.

        :param None:

        :return: (dict), the expectation
        """
        check_exist = self.column in df.columns
        self.message = self.message if self.value else _placeholder(
            _override_msg(
                f"Column {self.column} <$does_or_not> exist in the DataFrame",
                self.message,
            ),
            check_exist,
            "<$does_or_not>",
            ("does", "does not"),
        )

        if not check_exist:
            return {
                "has_failed": check_exist,
                "got": ", ".join(df.columns),
                "message": self.message,
            }

        if self.value:
            data_type = df.schema[self.column].dataType
            check_type = data_type == self.value
            self.message = _placeholder(
                _override_msg(
                    f"Column {self.column} exists in the DataFrame and <$is_or_not> of type: {self.value}",  # noqa: E501
                    self.message,
                ),
                check_type,
                "<$is_or_not>",
                ("is", "isn't"),
            )
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
