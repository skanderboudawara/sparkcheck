from abc import ABC, abstractmethod
from typing import Any

from pyspark.sql import Column, DataFrame

from ..ext._decorators import (
    check_column_exist,
    check_inputs,
    validate_expectation,
)


class ColumnsExpectations(ABC):  # pragma: no cover
    @check_inputs
    def __init__(
        self,
        col_name: str | Column,
        message: str | None = None,
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
    def eval_expectation(self, target: DataFrame) -> dict: ...

    @abstractmethod
    def get_message(self, check: bool) -> None: ...


class DataFrameExpectation(ABC):  # pragma: no cover
    @check_inputs
    def __init__(
        self,
        message: str | None = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        self.message = message

    @abstractmethod
    def get_message(self, check: bool) -> None: ...

    @abstractmethod
    def eval_expectation(self, target: DataFrame) -> dict: ...
