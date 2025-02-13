from __future__ import annotations

import os
import re
from typing import Any, cast, overload

from pyspark.sql import Column, DataFrame, Row
from pyspark.sql.functions import col, lit
from pyspark.sql.types import DecimalType

from sparkchecker.constants import OPERATOR_MAP


def _lit_or_raw(
    value: str | bool | float | Column,
    is_escaped: bool = False,
    default: str = "lit",
) -> str | bool | float | Column:  # pragma: no cover
    """
    THIS FUNCTION IS NOT INTENDED TO BE USED DIRECTLY.

    Convert a `value` to a literal or raw string.

    :param value: (str | bool | float | Column), a value to convert
    :param is_escaped: (bool), flag to determine if the value should be treated
        as a raw string literal or not
    :param default: (str), flag to determine if the value should be treated
    :returns: (str | bool | float | Column) a spark Column or raw str
    """
    if is_escaped:
        value = rf"{value}"
    return lit(value) if default == "lit" else value


@overload
def to_col(
    column_name: str | None,
    is_col: bool = True,
    escaped: bool = False,
    default: str = "lit",
) -> Column: ...  # pragma: no cover


@overload
def to_col(
    column_name: bool | float | Column,
    is_col: bool = True,
    escaped: bool = False,
    default: str = "raw",
) -> Column: ...  # pragma: no cover


def to_col(
    column_name: str | bool | float | Column | None,
    is_col: bool = True,
    escaped: bool = False,
    default: str = "lit",
) -> str | bool | float | Column:
    """
    Convert a `column_name` string to a column.

    :param column_name: (str | bool | float | Column), `pyspark.sql.Column`
        or a column name
    :param is_col: (bool), flag to determine if the column should be treated
        as a column or literal
    :param default: (str), flag to determine if the column should be treated
    :param escaped: (bool), flag to determine if the column should be treated
        as a raw string literal or not
    :returns: (str | bool | float | Column) a spark Column or raw str
    :raises: (TypeError), If the input is not a string, float, or Column
    """
    if default not in {"lit", "raw"}:
        raise ValueError(
            "Argument `default` must be one of 'lit' or 'raw' but got: ",
            default,
        )
    if column_name is None:
        return _lit_or_raw("NoneObject", is_escaped=False, default=default)

    # Handle string column names
    if isinstance(column_name, str):
        if is_col:
            return col(column_name)
        return _lit_or_raw(column_name, is_escaped=escaped, default=default)

    # Handle numeric types (int, float, bool)
    if isinstance(column_name, int | float | bool):
        return _lit_or_raw(column_name, is_escaped=False, default=default)

    # Handle Column type directly
    if isinstance(column_name, Column):
        return column_name

    # Raise an error if none of the conditions match
    raise TypeError(
        "Argument `column_name` must be of type "
        "`str`, `float`, `Column`, or `None`, but got: {type(column_name)}",
    )


def sanitize_column_name(input_string: Any) -> Any | Column:
    """
    Convert a column name to backticks format.

    :param input_string: (str), a column name
    :returns: (str) a column name in backticks format
    """
    if (
        isinstance(input_string, str)
        and input_string.startswith("`")
        and input_string.endswith("`")
    ):
        return to_col(input_string.strip("`"), is_col=True)
    return input_string


def to_name(column: str | Column | bool | float | None) -> str:
    """
    Convert a `column` to a column name.

    :param column: (str | Column | bool | float | None), a spark Column
    :returns: (str) a column name
    :raises: (TypeError), If the input is not a string or Column
    """
    if column is None:
        return "NoneObject"
    if isinstance(column, str | bool | float | int):
        return str(column)
    if isinstance(column, Column):
        return column._jc.toString()  # noqa: SLF001
    raise TypeError(
        "Argument `column` must be of type str | Column but got: ",
        type(column),
    )


def _op_check(self: object, operator: str) -> None:
    """
    Check if the operator is valid.

    :param operator: (str), the operator to check
    :returns: None
    :raises: (ValueError), If the operator is not valid.
    """
    class_name = self.__class__.__name__
    if operator not in OPERATOR_MAP:
        valid_operators = ", ".join(OPERATOR_MAP.keys())
        raise ValueError(
            f"{class_name}: Invalid operator: '{operator}'. "
            f"Must be one of: '[{valid_operators}]'",
        )


def to_decimal(decimal_string: str) -> DecimalType:
    """
    Parse decimal string format (e.g. 'decimal(10,2)') into DecimalType object.

    :param decimal_string: (str), string like 'decimal(10,2)'.

    :return: (DecimalType), a DecimalType object.
    :raises ValueError: If the input string is not in the correct format.
    """
    # Regular expression to match 'decimal(precision,scale)'
    match = re.fullmatch(
        r"^decimal\((\d+),\s*?(\d+)\)$",
        decimal_string.strip().lower(),
    )

    if not match:
        raise ValueError(
            f"Invalid decimal type string: {decimal_string},"
            " it should be written like `decimal(10, 2)`",
        )

    precision, scale = map(int, match.groups())
    return DecimalType(precision=precision, scale=scale)


def split_base_file(file_path: str) -> tuple[str, str]:
    r"""
    Extracts the base path and the filename from a given file path.

    :param file_path (str): The file path to extract the base path
        and filename from.
    :returns: (tuple[str, str]) The base path and the filename with
        '_expectations_result.log' appended.
    """
    base_path = os.path.dirname(file_path)
    filename = os.path.splitext(os.path.basename(file_path))[0]
    new_filename = f"{filename}_sparkchecker_result.log"
    full_path = os.path.join(base_path, new_filename)
    full_path_normalized = os.path.normpath(full_path)
    return filename, full_path_normalized


def _substitute(input_string: str, condition: bool, placeholder: str) -> str:
    """
    Replace placeholder in string with conditional text based on boolean value.

    The placeholder is in the format "<$text1|text2>", where "text1" is used
    if the condition is True, and "text2" is used if the condition is False.

    :param input_string (str): The string containing the placeholder.
    :param condition (bool): The condition to determine the replacement value.
    :param placeholder (str): The placeholder to replace (e.g., "<$is|not>").
    :returns: (str) The modified string with the placeholder replaced.
    :raises: (TypeError), If the input string is not a string.
    :raises: (TypeError), If the condition is not a boolean.
    :raises: (TypeError), If the placeholder is not a string.
    :raises: (ValueError), If the placeholder format is invalid.
    """
    if not isinstance(input_string, str):
        raise TypeError(
            "Argument `input_string` must be of type str but got: ",
            type(input_string),
        )
    if not isinstance(condition, bool):
        raise TypeError(
            "Argument `condition` must be of type bool but got: ",
            type(condition),
        )
    if not isinstance(placeholder, str):
        raise TypeError(
            "Argument `placeholder` must be of type str but got: ",
            type(placeholder),
        )
    match = re.match(r"<\$(.*?)\|(.*?)>", placeholder)
    if not match:
        raise ValueError(
            "Invalid placeholder format. Must be in the "
            "format '<$text1|text2>'.",
        )

    text1, text2 = match.groups()
    replacement = text1 if condition else text2
    return input_string.replace(placeholder, replacement)


def _resolve_msg(default: str, msg: str | None) -> str:
    """
    Returns provided message if not None, otherwise returns default message.

    :param default: (str), the default message to use if `msg` is None.
    :param msg: (str | None), the message to override
        the default message.
    :return: (str), the resulting message.
    :raises: (TypeError), If the default message is not a string.
    :raises: (TypeError), If the message is not a string or None.
    """
    if not isinstance(default, str):
        raise TypeError(
            "Argument `default` must be of type str but got: ",
            type(default),
        )
    if not isinstance(msg, str | type(None)):
        raise TypeError(
            "Argument `msg` must be of type str | None but got: ",
            type(msg),
        )
    return msg if msg is not None else default


def eval_first_fail(
    df: DataFrame,
    column: str | Column,
    expectation: Column,
) -> tuple[bool, int, dict]:
    """
    This function evaluates the expectation on the DataFrame.

    :param df: (DataFrame), the DataFrame to check
    :param column: (str | Column), the column to check
    :param expectation: (Column), the expectation to check
    :return: (tuple), the check, the count of cases and the first failed row
    :raises: (TypeError), if the expectation is not of type Column
    :raises: (TypeError), if the DataFrame is not of type DataFrame
    """
    if not isinstance(expectation, Column):
        raise TypeError(
            "Argument `expectation` must be of type Column but got: ",
            type(expectation),
        )
    if not isinstance(df, DataFrame):
        raise TypeError(
            "Argument `df` must be of type DataFrame but got: ",
            type(df),
        )
    column = to_col(column)
    # We need to check the opposite of our expectations
    df = df.filter(~expectation).select(column)
    if not eval_empty_dataframe(df):
        first_failed_row = cast(Row, df.first())
        count_cases = df.count()
        return True, count_cases, first_failed_row.asDict()
    return False, 0, {}


def eval_empty_dataframe(target: DataFrame) -> bool:  # pragma: no cover
    """
    This function evaluates if a DataFrame is empty.

    This method ensures compatibility with Spark 3.3 and below.

    :param target: (DataFrame), the DataFrame to check

    :return: (bool), True if the DataFrame is empty, False otherwise
    """
    spark_version = target.sparkSession.version
    if spark_version >= "3.3":
        return target.isEmpty()
    return target.rdd.isEmpty()


def _format_column_name(input_string: str) -> str:
    """
    This function formats a column name inside double backticks.

    :param input_string: (str), a column name
    :returns: (str), a column name in backticks format
    """
    # Regex to match text inside double backticks and format it
    return re.sub(r"``(.*?)``", r"column `\1`", input_string)
