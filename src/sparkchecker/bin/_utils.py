import os
import re
from typing import Union

import yaml
from pyspark.sql import Column
from pyspark.sql.functions import col, lit
from pyspark.sql.types import DecimalType

from sparkchecker.bin._constants import OPERATOR_MAP


def str_to_col(column_name: Union[str, Column], is_col: bool = True) -> Column:
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
        return column._jc.toString()
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
        return list(str_to_col(list_args, is_col))
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
        f"Invalid decimal type string: {decimal_string}, it should be writtend `decimal(1, 2)`",
    )


def extract_base_path_and_filename(file_path):
    """
    Extracts the base path and the filename from a given file path.

    """
    base_path = os.path.dirname(file_path)
    filename = os.path.splitext(os.path.basename(file_path))[
        0
    ]  # Extract filename without extension
    new_filename = f"{filename}_expectations_result.log"  # Append .log extension
    return filename, os.path.join(base_path, new_filename)


def _placeholder(
    input_string: str,
    condition: bool,
    placeholder: str,
    replacements: tuple[str, str],
) -> str:
    """
    Replaces the specified placeholder in a string based on a boolean condition.

    :param input_string (str): The string containing the placeholder.
    :param condition (bool): The condition to determine the replacement value.
    :param placeholder (str): The placeholder to replace (e.g., "<$is_or_not>").
    :param replacements (tuple[str, str]): A tuple containing the replacements for True and False conditions.

    :returns: (str) The modified string with the placeholder replaced.
    """
    replacement = replacements[0] if condition else replacements[1]
    return input_string.replace(placeholder, replacement)


def _overrid_msg(default, msg):
    return default if not msg else msg
