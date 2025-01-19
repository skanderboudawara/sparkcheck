"""
The base classes for the column-based and DataFrame-based expectations.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pyspark.sql import Column, DataFrame

from ..ext._decorators import (
    add_class_prefix,
    check_column_exist,
    check_dataframe,
    check_inputs,
    validate_expectation,
)


class ColumnsExpectations(ABC):  # pragma: no cover
    """
    An abstract class for column-based expectations.
    """

    @check_inputs
    def __init__(
        self,
        col_name: str | Column,
        message: str | None = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        """
        The constructor for the ColumnsExpectations class.

        :param col_name: (str | Column), The column name or the column object.
        :param message: (str | None), The message to be displayed in
            case of failure.
        :param kwargs: (Any), The additional arguments to be
            passed to the class.
        :return: (None)
        """
        self.column = col_name
        self.message = message

    @property
    @abstractmethod
    def constraint(self) -> Column: ...

    @abstractmethod
    @validate_expectation
    @check_dataframe
    @check_column_exist
    def eval_expectation(self, target: DataFrame) -> dict: ...

    @abstractmethod
    @add_class_prefix
    def get_message(self, check: bool) -> None: ...


class DataFrameExpectation(ABC):  # pragma: no cover
    """
    The abstract class for DataFrame-based expectations.
    """

    @check_inputs
    def __init__(
        self,
        message: str | None = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        """
        The constructor for the DataFrameExpectation class.

        :param message: (str | None), The message to be displayed
            in case of failure.
        :param kwargs: (Any), The additional arguments to be passed
            to the class.
        :return: (None)
        """
        self.message = message

    @abstractmethod
    @add_class_prefix
    def get_message(self, check: bool) -> None: ...

    @abstractmethod
    @validate_expectation
    @check_dataframe
    def eval_expectation(self, target: DataFrame) -> dict: ...
