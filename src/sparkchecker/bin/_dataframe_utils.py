from abc import ABC, abstractmethod
from typing import Any, Union

from pyspark.sql import DataFrame
from pyspark.sql.types import DataType

from sparkchecker.bin._constants import OPERATOR_MAP
from sparkchecker.bin._decorators import validate_predicate
from sparkchecker.bin._utils import _check_operator, _overrid_msg, _placeholder


class _DataFrameExpectation(ABC):
    def __init__(
        self,
        df: Union[DataFrame, None] = None,
        message: Union[str, None] = None,
        **kwargs: Any,
    ) -> None:
        self.df: Union[DataFrame, None] = df
        if message and not isinstance(message, str):
            raise TypeError(
                "Argument message must be of type str but got: ",
                type(message),
                f"for {message!r}",
            )
        self.message = message

    @property
    @abstractmethod
    def predicate(self) -> bool: ...


class _IsEmpty(_DataFrameExpectation):
    def __init__(
        self,
        df: Union[DataFrame, None] = None,
        message: Union[str, None] = None,
        **kwargs: Any,
    ) -> None:
        """
        This class checks if a DataFrame is empty.

        :param df: (DataFrame), the DataFrame to check

        :return: None
        """
        super().__init__(df, message)

    @property
    @validate_predicate
    def predicate(self) -> bool:
        """
        This method returns the predicate.

        :param None:

        :return: (bool), the predicate
        """
        check = self.df.isEmpty()
        self.message = _placeholder(
            _overrid_msg("The DataFrame  <$is_or_not> empty", self.message),
            check,
            "<$is_or_not>",
            ("is", "isn't"),
        )
        return {"predicate": check, "got": check, "message": self.message}


class _IsNotEmpty(_DataFrameExpectation):
    def __init__(
        self,
        df: Union[DataFrame, None] = None,
        message: Union[str, None] = None,
        **kwargs: Any,
    ) -> None:
        """
        This class checks if a DataFrame is empty.

        :param df: (DataFrame), the DataFrame to check

        :return: None
        """
        super().__init__(df, message)

    @property
    @validate_predicate
    def predicate(self) -> bool:
        """
        This method returns the predicate.

        :param None:

        :return: (bool), the predicate
        """
        check = not (self.df.isEmpty())
        self.message = _placeholder(
            _overrid_msg("The DataFrame  <$is_or_not> empty", self.message),
            check,
            "<$is_or_not>",
            ("isn't", "is"),
        )
        return {"predicate": check, "got": check, "message": self.message}


class _CountThreshold(_DataFrameExpectation):
    def __init__(
        self,
        constraint: int,
        operator: str,
        df: Union[DataFrame, None] = None,
        message: Union[str, None] = None,
        **kwargs: Any,
    ) -> None:
        """
        This class compares the count of a DataFrame to a constraint.

        :param df: (DataFrame), the DataFrame to check

        :param constraint: (int), the constraint to check

        :return: None
        """
        _check_operator(operator)
        if not isinstance(constraint, int):
            raise TypeError(
                "Argument for DataFrame count must be of type int but got: ",
                type(constraint),
            )
        self.constraint = constraint
        self.operator = operator
        super().__init__(df, message)

    @property
    @validate_predicate
    def predicate(self) -> bool:
        """
        This method returns the predicate.

        :param None:

        :return: (bool), the predicate
        """
        count = self.df.count()
        # Convert the threshold to a literal value and apply the operator
        check = OPERATOR_MAP[self.operator](count, self.constraint)
        self.message = _placeholder(
            _overrid_msg(
                f"The DataFrame has {count} rows, which <$is_or_not> {self.operator} than {self.constraint}",
                self.message,
            ),
            check,
            "<$is_or_not>",
            ("is", "isn't"),
        )
        return {"predicate": check, "got": count, "message": self.message}


class _PartitionsCount(_DataFrameExpectation):
    def __init__(
        self,
        constraint: int,
        operator: str,
        message: Union[str, None] = None,
        df: Union[DataFrame, None] = None,
        **kwargs: Any,
    ) -> None:
        """
        This class compares the number of partitions of a DataFrame to a constraint.

        :param df: (DataFrame), the DataFrame to check

        :param constraint: (int), the constraint to check

        :return: None
        """
        _check_operator(operator)
        if not isinstance(constraint, int):
            raise TypeError(
                "Argument for DataFrame Partitions must be of type int but got: ",
                type(constraint),
            )
        self.constraint = constraint
        self.operator = operator
        super().__init__(df, message)

    @property
    @validate_predicate
    def predicate(self) -> bool:
        """
        This method returns the predicate.

        :param None:

        :return: (bool), the predicate
        """
        rdd_count = self.df.rdd.getNumPartitions()
        # Convert the threshold to a literal value and apply the operator
        check_count = OPERATOR_MAP[self.operator](rdd_count, self.constraint)
        self.message = _placeholder(
            _overrid_msg(
                f"The DataFrame has {rdd_count} partitions, which <$is_or_not> {self.operator} than {self.constraint}",
                self.message,
            ),
            check_count,
            "<$is_or_not>",
            ("is", "isn't"),
        )
        return {"predicate": check_count, "got": rdd_count, "message": self.message}


class _Exist(_DataFrameExpectation):
    def __init__(
        self,
        column: str,
        message: Union[str, None] = None,
        df: Union[DataFrame, None] = None,
        constraint: Union[DataType, None] = None,
        **kwargs: Any,
    ) -> None:
        """
        This class checks if a column exists in a DataFrame.

        :param df: (DataFrame), the DataFrame to check

        :param column: (str), the column to check

        :param constraint: (DataType), the constraint to check

        :return: None
        """
        if constraint and not isinstance(constraint, DataType):
            raise TypeError(
                "Argument for DataFrame Partitions must be of type int but got: ",
                type(constraint),
            )
        self.constraint = constraint
        self.column = column
        super().__init__(df, message)

    @property
    @validate_predicate
    def predicate(self) -> bool:
        """
        This method returns the predicate.

        :param None:

        :return: (bool), the predicate
        """
        check_exist = self.column in self.df.columns
        self.message = _placeholder(
            _overrid_msg(
                f"Column {self.column} <$does_or_not> exists in the DataFrame",
                self.message,
            ),
            check_exist,
            "<$does_or_not>",
            ("does", "does not"),
        )
        if self.column in self.df.columns:
            return {
                "predicate": check_exist,
                "got": " ,".join(list(self.df.columns)),
                "message": self.message,
            }
        if self.constraint:
            data_type = next(
                field.dataType
                for field in self.df.schema.fields
                if field.name == self.column
            )
            check_type = data_type == self.constraint
            self.message = _placeholder(
                _overrid_msg(
                    f"Column {self.column} exists in the DataFrame and <$is_or_not> of type: {self.constraint}",
                    self.message,
                ),
                check_type,
                "<$is_or_not>",
                ("is", "isn't"),
            )
            return {
                "predicate": check_type,
                "got": data_type,
                "message": self.message,
            }
        return {
            "predicate": check_exist,
            "got": " ,".join(list(self.df.columns)),
            "message": self.message,
        }
