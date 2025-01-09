import os
import re
from typing import Union

from pyspark.sql import Column
from pyspark.sql.functions import col, lit
from pyspark.sql.types import DecimalType

from sparkchecker.bin._constants import OPERATOR_MAP


def str_to_col(column_name: Union[str, float, Column], is_col: bool = True) -> Column:
    """
    Convert a `column_name` string to a column.

    :param column_name: (Union[str, Column]), `pyspark.sql.Column` or a column name

    :returns: (Column) a spark Column
    """
    if isinstance(column_name, str):
        return col(column_name) if is_col else lit(column_name)
    if isinstance(column_name, Column):
        return column_name
    return lit(column_name)


def col_to_name(column: Union[str, Column]) -> str:
    """
    Convert a `column` to a column name.

    :param column: (Column), a spark Column

    :returns: (str) a column name
    """
    if isinstance(column, str):
        return column
    if isinstance(column, Column):
        return column._jc.toString()  # noqa: SLF001
    raise TypeError(
        "Argument `column` must be of type Union[str, Column] but got: ",
        type(column),
    )


def args_to_list_cols(
    list_args: Union[
        str,
        Column,
        list[Column],
        list[str],
        tuple[str, ...],
        tuple[Column, ...],
    ],
    is_col: bool = True,
) -> list[Column]:
    """
    Convert a `list_args` to a list[Columns].

    :param list_args: (Union[str, Column, list[Column], list[str], tuple[str, ...],
        tuple[Column, ...]]), a list of arguments

    :returns: (list[Column]), a list of Column
    """
    if isinstance(list_args, (str, Column)):
        return [str_to_col(list_args, is_col)]
    if not isinstance(list_args, (list, tuple)):
        raise TypeError(
            "Argument `list_args` must be of type \
                Union[str, Column, list[Column], list[str], tuple[str,...], tuple[Column, ...]] \
                    but got: ",
            type(list_args),
        )
    if not all(isinstance(arg, (str, Column)) for arg in list_args):
        raise TypeError(
            "All elements of `list_args` must be of type Union[str, Column] but got: ",
            [type(arg) for arg in list_args],
        )
    return [str_to_col(arg, is_col) for arg in list_args]


def _check_operator(operator: str) -> None:
    """
    This function checks if the operator is valid.

    :param operator: (str), the operator to check

    :return: None
    """
    if operator not in OPERATOR_MAP:
        raise ValueError(
            f"Invalid operator: {operator}. Must be one of {list(OPERATOR_MAP.keys())}",
        )


def parse_decimal_type(decimal_string: str) -> DecimalType:
    """
    Parses a string like 'decimal(10,2)' and converts it to a DecimalType object.

    :param decimal_string: (str), string like 'decimal(10,2)'.

    :return: (DecimalType), a DecimalType object.
    """
    # Regular expression to match 'decimal(precision,scale)'
    match = re.fullmatch(
        r"^decimal\((\d+),\s*?(\d+)\)$",
        decimal_string.strip().lower(),
    )

    if match:
        precision, scale = map(int, match.groups())
        return DecimalType(precision=precision, scale=scale)
    raise ValueError(
        f"Invalid decimal type string: {decimal_string}, it should be written like `decimal(1, 2)`",
    )


def extract_base_path_and_filename(file_path: str) -> tuple[str, str]:
    """
    Extracts the base path and the filename from a given file path.

    :param file_path (str): The file path to extract the base path and filename from.

    :returns: (tuple[str, str]) The base path and the filename.
    """
    base_path = os.path.dirname(file_path)
    filename = os.path.splitext(os.path.basename(file_path))[
        0
    ]  # Extract filename without extension
    new_filename = f"{filename}_expectations_result.log"  # Append .log extension
    return filename, os.path.join(base_path, new_filename)


def _placeholder(input_string: str, condition: bool, placeholder: str) -> str:
    """
    Replaces the specified placeholder in a string based on a boolean condition.

    The placeholder is in the format "<$text1|text2>", where "text1" is used
    if the condition is True, and "text2" is used if the condition is False.

    :param input_string (str): The string containing the placeholder.

    :param condition (bool): The condition to determine the replacement value.

    :param placeholder (str): The placeholder to replace (e.g., "<$is|not>").

    :returns: (str) The modified string with the placeholder replaced.
    """
    match = re.match(r"<\$(.*?)\|(.*?)>", placeholder)
    if not match:
        raise ValueError(
            "Invalid placeholder format. Must be in the format '<$text1|text2>'.",
        )

    text1, text2 = match.groups()
    replacement = text1 if condition else text2
    return input_string.replace(placeholder, replacement)


def _override_msg(default: str, msg: Union[str, None]) -> str:
    """
    This function returns the default message if the message is None.

    :param default: (str), the default message

    :param msg: (Union[str, None]), the message

    :return: (str), the message
    """
    return msg if msg else default
