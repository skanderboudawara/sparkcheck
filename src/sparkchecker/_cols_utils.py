from abc import ABC, abstractmethod
from typing import Union

from pyspark.sql import Column
from pyspark.sql.functions import lit

from sparkchecker._cols_utils import _args_to_list_cols, _check_operator, _str_to_col
from sparkchecker._constants import OPERATOR_MAP


class _ColumnsExpectations(ABC):
    def __init__(self, column: Union[str, Column]) -> None:
        self.column: Column = column

    @property
    @abstractmethod
    def predicate(self) -> bool: ...


class _NonNullColumn(_ColumnsExpectations):
    def __init__(self, column: Union[str, Column]) -> None:
        super().__init__(_str_to_col(column))

    @property
    def predicate(self) -> bool:
        self.column.isNotNull()


class _NullColumn(_ColumnsExpectations):
    def __init__(self, column: Union[str, Column]) -> None:
        super().__init__(_str_to_col(column))

    @property
    def predicate(self) -> bool:
        self.column.isNull()


class _RlikeColumn(_ColumnsExpectations):
    def __init__(self, column: Union[str, Column], pattern: str) -> None:
        super().__init__(_str_to_col(column))
        self.pattern = pattern

    @property
    def predicate(self) -> bool:
        self.column.rlike(self.pattern)


class _IsInColumn(_ColumnsExpectations):
    def __init__(self, column: Union[str, Column], array: list) -> None:
        super().__init__(_str_to_col(column))
        self.array = _args_to_list_cols(array, is_lit=True)

    @property
    def predicate(self) -> bool:
        self.column.isin(*self.array)


class _ColumnCompare:
    def __init__(
        self, column: Column, threshold: Union[str, float], operator: str,
    ) -> None:
        _check_operator(operator)
        self.threshold = _str_to_col(threshold, is_lit=True)
        self.operator = operator
        super().__init__(_str_to_col(column))

    @property
    def predicate(self) -> bool:
        # Convert the threshold to a literal value and apply the operator
        return OPERATOR_MAP[self.operator](self.column, lit(self.threshold))
