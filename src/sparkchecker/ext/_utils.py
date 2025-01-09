import os
import re
from typing import Union

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import DecimalType

from sparkchecker.constants import OPERATOR_MAP


def str_to_col(
    column_name: Union[str, bool, float, Column],
    is_col: bool = True,
) -> Column:
    """
    Convert a `column_name` string to a column.

    :param column_name: (Union[str, bool, float, Column]), `pyspark.sql.Column` or a column name
    :param is_col: (bool), flag to determine if the column should be treated as a column or literal
    :returns: (Column) a spark Column
    :raises: (TypeError), If the input is not a string, float, or Column

    Examples:
    >>> str_to_col('column1')
    Column<b'column1'>

    >>> str_to_col('column1', is_col=False)
    Column<b'column1'>

    >>> str_to_col(123)
    Column<b'123'>

    >>> str_to_col(col('column1'))
    Column<b'column1'>

    >>> str_to_col(123.0)
    Column<b'123.0'>

    >>> str_to_col(123.0, is_col=False)
    Column<b'123.0'>

    >>> str_to_col(123.0, is_col=True)
    Column<b'123.0'>

    """
    if isinstance(column_name, str):
        return col(column_name) if is_col else lit(column_name)
    if isinstance(column_name, (float, int, bool)):
        return lit(column_name)
    if isinstance(column_name, Column):
        return column_name
    raise TypeError(
        "Argument `column_name` must be of type Union[str, float, Column] but got: ",
        type(column_name),
    )


def col_to_name(column: Union[str, Column]) -> str:
    """
    Convert a `column` to a column name.

    :param column: (Column), a spark Column
    :returns: (str) a column name
    :raises: (TypeError), If the input is not a string or Column

    Examples:
    >>> col_to_name('column1')
    'column1'

    >>> col_to_name(col('column1'))
    'column1'

    >>> col_to_name(123)
    Traceback (most recent call last):
        ...
    TypeError: Argument `column` must be of type Union[str, Column] but got:  <class 'int'>

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
        float,
        str,
        Column,
        list[Union[str, Column]],
        tuple[Union[str, Column], ...],
    ],
    is_col: bool = True,
) -> list[Column]:
    """
    Convert `list_args` to a list of Columns.

    :param list_args:
        (Union[float, str, Column, list[Union[str, float, Column]], tuple[Union[str, Column], ...]])
        a list or tuple of arguments that can be strings or Columns.
    :param is_col: (bool), flag to determine if the strings should be treated as
        column names or literals.
    :returns: (list[Column]), a list of Columns.
    :raises: (TypeError), If the input is not a string, Column, list, or tuple.
    :raises: (TypeError), If the elements of the list are not strings or Columns.

    Examples:
    >>> args_to_list_cols('column1')
    [Column<b'column1'>]

    >>> args_to_list_cols(col('column1'))
    [Column<b'column1'>]

    >>> args_to_list_cols(['column1', 'column2'])
    [Column<b'column1'>, Column<b'column2'>]

    >>> args_to_list_cols(('column1', 'column2'))
    [Column<b'column1'>, Column<b'column2'>]

    >>> args_to_list_cols(['column1', col('column2')])
    [Column<b'column1'>, Column<b'column2'>]

    >>> args_to_list_cols('column1', is_col=False)
    [Column<b'column1'>]

    """
    if isinstance(list_args, (str, float, Column)):
        return [str_to_col(list_args, is_col)]
    if not isinstance(list_args, (list, tuple)):
        raise TypeError(
            "Argument `list_args` must be of type Union[str, Column, list[Union[str, Column]]",
            f" tuple[Union[str, Column], ...]] but got: {type(list_args)}",
        )
    if not all(isinstance(arg, (str, Column, float)) for arg in list_args):
        raise TypeError(
            "All elements of `list_args` must be of type Union[str, Column] but got:",
            f" {[type(arg) for arg in list_args]}",
        )
    return [str_to_col(arg, is_col) for arg in list_args]


def _check_operator(operator: str) -> None:
    """
    Check if the operator is valid.

    :param operator: (str), the operator to check
    :returns: None
    :raises: (ValueError), If the operator is not valid.

    Example:
    >>> OPERATOR_MAP = {'+': 'add', '-': 'subtract'}

    >>> _check_operator('+')

    >>> _check_operator('/')
    Traceback (most recent call last):
    ...
    ValueError: Invalid operator: '/'. Must be one of: +, -

    """
    if operator not in OPERATOR_MAP:
        valid_operators = ", ".join(OPERATOR_MAP.keys())
        raise ValueError(
            f"Invalid operator: '{operator}'. Must be one of: {valid_operators}",
        )


def parse_decimal_type(decimal_string: str) -> DecimalType:
    """
    Parses a string like 'decimal(10,2)' and converts it to a DecimalType object.

    :param decimal_string: (str), string like 'decimal(10,2)'.

    :return: (DecimalType), a DecimalType object.
    :raises ValueError: If the input string is not in the correct format.

    Examples:
    >>> parse_decimal_type('decimal(10,2)')
    DecimalType(precision=10, scale=2)

    >>> parse_decimal_type('decimal(5, 0)')
    DecimalType(precision=5, scale=0)

    >>> parse_decimal_type('decimal(15, 5)')
    DecimalType(precision=15, scale=5)

    >>> parse_decimal_type('invalid')
    Traceback (most recent call last):
        ...
    ValueError: Invalid decimal type string: invalid, it should be written like `decimal(10, 2)`

    """
    # Regular expression to match 'decimal(precision,scale)'
    match = re.fullmatch(
        r"^decimal\((\d+),\s*?(\d+)\)$",
        decimal_string.strip().lower(),
    )

    if not match:
        raise ValueError(
            f"Invalid decimal type string: {decimal_string},",
            " it should be written like `decimal(10, 2)`",
        )

    precision, scale = map(int, match.groups())
    return DecimalType(precision=precision, scale=scale)


def extract_base_path_and_filename(file_path: str) -> tuple[str, str]:
    """
    Extracts the base path and the filename from a given file path.

    :param file_path (str): The file path to extract the base path and filename from.
    :returns: (tuple[str, str]) The base path and the filename with
        '_expectations_result.log' appended.

    Examples:
    >>> extract_base_path_and_filename('/path/to/file.txt')
    ('/path/to', 'file_expectations_result.log')

    >>> extract_base_path_and_filename('file.txt')
    ('', 'file_expectations_result.log')

    """
    base_path = os.path.dirname(file_path)
    filename = os.path.splitext(os.path.basename(file_path))[0]
    new_filename = f"{filename}_expectations_result.log"
    return filename, os.path.join(base_path, new_filename)


def _substitute(input_string: str, condition: bool, placeholder: str) -> str:
    """
    Replaces the specified placeholder in a string based on a boolean condition.

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

    Examples:
    >>> _substitute("This is <$is|not> a test.", True, "<$is|not>")
    'This is is a test.'

    >>> _substitute("This is <$is|not> a test.", False, "<$is|not>")
    'This is not a test.'

    >>> _substitute("The value is <$high|low>.", True, "<$high|low>")
    'The value is high.'

    >>> _substitute("The value is <$high|low>.", False, "<$high|low>")
    'The value is low.'

    >>> _substitute("Invalid placeholder <$invalid>", True, "<$invalid>")
    Traceback (most recent call last):
        ...
    ValueError: Invalid placeholder format. Must be in the format '<$text1|text2>'.

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
            "Invalid placeholder format. Must be in the format '<$text1|text2>'.",
        )

    text1, text2 = match.groups()
    replacement = text1 if condition else text2
    return input_string.replace(placeholder, replacement)


def _resolve_msg(default: str, msg: Union[str, None]) -> str:
    """
    Returns the provided message if it is not None, otherwise returns the default message.

    :param default: (str), the default message to use if `msg` is None.
    :param msg: (Union[str, None]), the message to override the default message.
    :return: (str), the resulting message.
    :raises: (TypeError), If the default message is not a string.
    :raises: (TypeError), If the message is not a string or None.

    Examples:
    >>> _resolve_msg("Default message", "Custom message")
    'Custom message'

    >>> _resolve_msg("Default message", None)
    'Default message'

    >>> _resolve_msg(123, "Custom message")
    Traceback (most recent call last):
        ...
    TypeError: Argument `default` must be of type str but got:  <class 'int'>

    >>> _resolve_msg("Default message", 123)
    Traceback (most recent call last):
        ...
    TypeError: Argument `msg` must be of type Union[str, None] but got:  <class 'int'>

    """
    if not isinstance(default, str):
        raise TypeError(
            "Argument `default` must be of type str but got: ",
            type(default),
        )
    if not isinstance(msg, (str, type(None))):
        raise TypeError(
            "Argument `msg` must be of type Union[str, None] but got: ",
            type(msg),
        )
    return msg if msg is not None else default


def evaluate_first_fail(
    df: DataFrame,
    column: Union[str, Column],
    expectation: Column,
) -> tuple[bool, int, dict]:
    """
    This function evaluates the expectation on the DataFrame.

    :param df: (DataFrame), the DataFrame to check
    :param column: (Union[str, Column]), the column to check
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
    column = str_to_col(column)
    # We need to check the opposite of our expectations
    df = df.select(column).filter(~expectation)
    first_failed_row = df.first()
    check = bool(not first_failed_row)
    count_cases = df.count() if check else 0
    return check, count_cases, first_failed_row.asDict() if first_failed_row else {}
