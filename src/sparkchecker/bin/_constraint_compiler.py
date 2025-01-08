"""
This module contains the ConstraintCompiler class.
"""

from pyspark.sql import DataFrame

from sparkchecker.bin._cols_utils import (
    _ColumnCompare,
    _IsInColumn,
    _NonNullColumn,
    _NullColumn,
    _RlikeColumn,
)
from sparkchecker.bin._dataframe_utils import (
    _CountThreshold,
    _Exist,
    _IsEmpty,
    _PartitionsCount,
)

DATAFRAME_OPERATIONS = {
    "count": _CountThreshold,
    "partitions": _PartitionsCount,
    "is_empty": _IsEmpty,
    "exist": _Exist,
}
COLUMN_INSTANCES = {
    "not_null": _NonNullColumn,
    "is_null": _NullColumn,
    "pattern": _RlikeColumn,
    "in": _IsInColumn,
    "lower": _ColumnCompare,
    "lower_or_equal": _ColumnCompare,
    "equal": _ColumnCompare,
    "different": _ColumnCompare,
    "higher": _ColumnCompare,
    "higher_or_equal": _ColumnCompare,
}


class ConstraintCompiler:
    """
    This class compiles a stack of checks into a list of dictionaries.
    """

    def __init__(self, df: DataFrame, stack: list) -> None:
        """
        This class compiles a stack of checks into a list of dictionaries.

        :param df: (DataFrame), the DataFrame to check

        :param stack: (list), the stack of checks to compile

        :return: None
        """
        self.stack = stack
        self.df = df
        self.compiled_stack = []

    @staticmethod
    def _compile_dataframe_operation(df: DataFrame, check: dict) -> dict:
        """
        This static method compiles a dataframe operation check into a dictionary.

        :param df: (DataFrame), the DataFrame to check

        :param check: (dict), the check to compile

        :return: (dict), the compiled check
        """
        check_instance = DATAFRAME_OPERATIONS[check["check"]](df=df, **check)
        predicate = check_instance.predicate
        check.update(predicate)
        return check

    @staticmethod
    def _compile_column_operation(df: DataFrame, check: dict) -> dict:
        """
        This static method compiles a column operation check into a dictionary.

        :param df: (DataFrame), the DataFrame to check

        :param check: (dict), the check to compile

        :return: (dict), the compiled check
        """
        check_instance = COLUMN_INSTANCES[check["operator"]]
        check_instance = check_instance(**check)
        df = df.filter(~check_instance.predicate)
        example = df.first()
        predicate = bool(not example)
        count_cases = 0
        if predicate:
            count_cases = df.count()
        check.update(
            {
                "predicate": predicate.predicate,
                "count_cases": count_cases,
                "example": example,
            },
        )
        return check

    def compile(self) -> None:
        """
        This method compiles the stack of checks into a list of dictionaries.

        :param None

        :return: None
        """
        for check in self.stack:
            check_type = check["check"]
            compiled_check = check
            if check_type in DATAFRAME_OPERATIONS:
                compiled_check = self._compile_dataframe_operation(self.df, check)
            elif check_type == "column":
                compiled_check = self._compile_column_operation(self.df, check)
            else:
                raise ValueError(f"Unknown check type: {check_type}")
            self.compiled_stack.append(compiled_check)

    @property
    def compiled(self) -> list:
        """
        This property returns the compiled stack of checks.

        :param None

        :return: (list), the compiled stack of checks
        """
        return self.compiled_stack
