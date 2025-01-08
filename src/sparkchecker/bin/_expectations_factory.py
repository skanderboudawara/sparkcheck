"""
This module contains the ExpectationsFactory class.
"""

from pyspark.sql import DataFrame

from sparkchecker.bin._column_expectations import (
    ColumnCompare,
    IsInColumn,
    NonNullColumn,
    NullColumn,
    RlikeColumn,
)
from sparkchecker.bin._dataframe_expectations import (
    CountThreshold,
    Exist,
    IsEmpty,
    PartitionsCount,
)

DATAFRAME_OPERATIONS = {
    "count": CountThreshold,
    "partitions": PartitionsCount,
    "is_empty": IsEmpty,
    "exist": Exist,
}
COLUMN_INSTANCES = {
    "not_null": NonNullColumn,
    "is_null": NullColumn,
    "pattern": RlikeColumn,
    "in": IsInColumn,
    "lower": ColumnCompare,
    "lower_or_equal": ColumnCompare,
    "equal": ColumnCompare,
    "different": ColumnCompare,
    "higher": ColumnCompare,
    "higher_or_equal": ColumnCompare,
}


class ExpectationsFactory:
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
        expectation_instance = DATAFRAME_OPERATIONS[check["check"]](**check)
        expectation = expectation_instance.expectation(df)
        check.update(expectation)
        return check

    @staticmethod
    def _compile_column_operation(df: DataFrame, check: dict) -> dict:
        """
        This static method compiles a column operation check into a dictionary.

        :param df: (DataFrame), the DataFrame to check

        :param check: (dict), the check to compile

        :return: (dict), the compiled check
        """
        expectation_instance = COLUMN_INSTANCES[check["operator"]]
        expectation_instance = expectation_instance(**check)
        expectation, count_cases, example= expectation_instance.expectation(df)
        check.update(
            {
                "expectation": expectation.expectation,
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
