"""
This module contains the ExpectationsFactory class.
"""

from collections.abc import Mapping

from pyspark.sql import DataFrame

from ..ext._decorators import order_expectations_dict
from ._base import ColumnsExpectations, DataFrameExpectation
from ._column_expectations import (
    ColCompareCheck,
    ColIsInCheck,
    ColNonNullCheck,
    ColNullCheck,
    ColRegexLikeCheck,
)
from ._dataframe_expectations import (
    DataFrameCountThresholdCheck,
    DataFrameHasColumnsCheck,
    DataFrameIsEmptyCheck,
    DataFramePartitionsCountCheck,
)

DATAFRAME_CHECKS: Mapping[str, type[DataFrameExpectation]] = {
    "count": DataFrameCountThresholdCheck,
    "partitions": DataFramePartitionsCountCheck,
    "is_empty": DataFrameIsEmptyCheck,
    "has_columns": DataFrameHasColumnsCheck,
}
COLUMN_CHECKS: Mapping[str, type[ColumnsExpectations]] = {
    "not_null": ColNonNullCheck,
    "is_null": ColNullCheck,
    "pattern": ColRegexLikeCheck,
    "in": ColIsInCheck,
    "lower": ColCompareCheck,
    "lower_or_equal": ColCompareCheck,
    "equal": ColCompareCheck,
    "different": ColCompareCheck,
    "higher": ColCompareCheck,
    "higher_or_equal": ColCompareCheck,
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
        self.stack: list = stack
        self.df: DataFrame = df
        self.compiled_stack: list[dict] = []

    @staticmethod
    @order_expectations_dict
    def _compile_dataframe_operation(df: DataFrame, check: dict) -> dict:
        """
        This static method compiles a dataframe operation
            check into a dictionary.

        :param df: (DataFrame), the DataFrame to check
        :param check: (dict), the check to compile
        :return: (dict), the compiled check
        """
        expectation_instance = DATAFRAME_CHECKS[check["check"]](**check)
        expectation = expectation_instance.eval_expectation(target=df)
        check.update(expectation)
        return check

    @staticmethod
    @order_expectations_dict
    def _compile_column_operation(df: DataFrame, check: dict) -> dict:
        """
        This static method compiles a column operation check into a dictionary.

        :param df: (DataFrame), the DataFrame to check
        :param check: (dict), the check to compile
        :return: (dict), the compiled check
        """
        expectation_instance = COLUMN_CHECKS[check["operator"]](**check)
        expectation = expectation_instance.eval_expectation(target=df)
        check.update(expectation)
        return check

    def compile(self) -> None:
        """
        This method compiles the stack of checks into a list of dictionaries.

        :param None
        :return: None
        :raises: (ValueError), if the check type is unknown
        """
        if not self.stack:
            raise ValueError("No checks provided.")

        self.df = self.df.cache()  # To improve performance
        df_is_empty = self.df.isEmpty()

        for check in self.stack:
            check_type = check.get("check")
            if not check_type:
                raise ValueError(
                    "Check type is missing in the check dictionary.",
                )

            if check_type in DATAFRAME_CHECKS:
                compiled_check = self._compile_dataframe_operation(
                    self.df,
                    check,
                )
            elif check_type == "column":
                if df_is_empty:
                    compiled_check = {
                        "check": check_type,
                        "has_failed": True,
                        "message": (
                            "DataFrame is empty. No column checks can be "
                            "performed."
                        ),
                    }
                else:
                    compiled_check = self._compile_column_operation(
                        self.df,
                        check,
                    )
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
