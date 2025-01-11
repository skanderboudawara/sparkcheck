import os
import re
from typing import cast

from pyspark.sql import Column, DataFrame, Row
from pyspark.sql.functions import col, lit
from pyspark.sql.types import DecimalType

from sparkchecker.constants import OPERATOR_MAP


def to_col(
    column_name: str | bool | float | Column | None,
    is_col: bool = True,
    escaped: bool = False,
) -> Column:
    """
    Convert a `column_name` string to a column.

    :param column_name: (str | bool | float | Column), `pyspark.sql.Column`
        or a column name
    :param is_col: (bool), flag to determine if the column should be treated
        as a column or literal
    :param escaped: (bool), flag to determine if the column should be treated
        as a raw string literal or not
    :returns: (Column) a spark Column
    :raises: (TypeError), If the input is not a string, float, or Column

    Examples:
    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> to_col(None)
    Column<'NULL'>

    >>> to_col('column1')
    Column<'column1'>

    >>> to_col('column1', is_col=False)
    Column<'column1'>

    >>> to_col(123)
    Column<'123'>

    >>> to_col(col('column1'))
    Column<'column1'>

    >>> to_col(123.0)
    Column<'123.0'>

    >>> to_col(123.0, is_col=False)
    Column<'123.0'>

    >>> to_col(123.0, is_col=True)
    Column<'123.0'>

    >>> spark.stop()

    """
    if column_name is None:
        return lit(None)

    # Handle string column names
    if isinstance(column_name, str):
        if is_col:
            return col(column_name)
        if escaped:
            return lit(rf"{column_name}")
        return lit(column_name)

    # Handle numeric types (int, float, bool)
    if isinstance(column_name, int | float | bool):
        return lit(column_name)

    # Handle Column type directly
    if isinstance(column_name, Column):
        return column_name

    # Raise an error if none of the conditions match
    raise TypeError(
        "Argument `column_name` must be of type "
        "`str`, `float`, `Column`, or `None`, but got: {type(column_name)}",
    )


def to_name(column: str | Column | bool | float | None) -> str:
    """
    Convert a `column` to a column name.

    :param column: (str | Column | bool | float | None), a spark Column
    :returns: (str) a column name
    :raises: (TypeError), If the input is not a string or Column

    Examples:
    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> to_name('column1')
    'column1'

    >>> to_name(col('column1'))
    'column1'

    >>> to_name(None)
    'NULL'

    >>> to_name(1)
    '1'

    >>> to_name(True)
    'True'

    >>> spark.stop()

    """
    if column is None:
        return "NULL"
    if isinstance(column, str | bool | float | int):
        return str(column)
    if isinstance(column, Column):
        return column._jc.toString()  # noqa: SLF001
    raise TypeError(
        "Argument `column` must be of type str | Column but got: ",
        type(column),
    )


def _op_check(operator: str) -> None:
    """
    Check if the operator is valid.

    :param operator: (str), the operator to check
    :returns: None
    :raises: (ValueError), If the operator is not valid.

    Example:
    >>> _op_check('lower')

    """
    if operator not in OPERATOR_MAP:
        valid_operators = ", ".join(OPERATOR_MAP.keys())
        raise ValueError(
            f"Invalid operator: '{operator}'."
            f"Must be one of: {valid_operators}",
        )


def to_decimal(decimal_string: str) -> DecimalType:
    """
    Parses a string like 'decimal(10,2)' and converts it
        to a DecimalType object.

    :param decimal_string: (str), string like 'decimal(10,2)'.

    :return: (DecimalType), a DecimalType object.
    :raises ValueError: If the input string is not in the correct format.

    Examples:
    >>> to_decimal('decimal(10,2)')
    DecimalType(10,2)

    >>> to_decimal('decimal(5, 0)')
    DecimalType(5,0)

    >>> to_decimal('decimal(15, 5)')
    DecimalType(15,5)

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

    Examples:
    >>> split_base_file('/path/to/file.txt')[1] \
    ...     .replace('\\', '/')  # Normalize path for Windows
    '/path/to/file_sparkchecker_result.log'

    >>> split_base_file('/path/to/file.txt')[0]
    'file'

    >>> split_base_file('file.txt')
    ('file', 'file_sparkchecker_result.log')

    """
    base_path = os.path.dirname(file_path)
    filename = os.path.splitext(os.path.basename(file_path))[0]
    new_filename = f"{filename}_sparkchecker_result.log"
    full_path = os.path.join(base_path, new_filename)
    full_path_normalized = os.path.normpath(full_path)
    return filename, full_path_normalized


def _substitute(input_string: str, condition: bool, placeholder: str) -> str:
    """
    Replaces the specified placeholder in a string
        based on a boolean condition.

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

    >>> _substitute("This is <$is|is not> a test.", False, "<$is|is not>")
    'This is is not a test.'

    >>> _substitute("This is <$is|not> a test.", False, "<$is|not>")
    'This is not a test.'

    >>> _substitute("The value is <$high|low>.", True, "<$high|low>")
    'The value is high.'

    >>> _substitute("The value is <$high|low>.", False, "<$high|low>")
    'The value is low.'

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
    Returns the provided message if it is not None,
        otherwise returns the default message.

    :param default: (str), the default message to use if `msg` is None.
    :param msg: (str | None), the message to override
        the default message.
    :return: (str), the resulting message.
    :raises: (TypeError), If the default message is not a string.
    :raises: (TypeError), If the message is not a string or None.

    Examples:
    >>> _resolve_msg("Default message", "Custom message")
    'Custom message'

    >>> _resolve_msg("Default message", None)
    'Default message'

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

    Examples:
    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> df = spark.createDataFrame([(1, 2), (3, 4)], ['a', 'b'])
    >>> df = df.cache()

    >>> expectation = col('a') > 0
    >>> eval_first_fail(df, 'a', expectation)
    (False, 0, {})

    >>> expectation = col('a') > 3
    >>> eval_first_fail(df, 'a', expectation)
    (True, 2, {'a': 1})

    >>> expectation = col('a').isNotNull()
    >>> eval_first_fail(df, 'a', expectation)
    (False, 0, {})

    >>> expectation = col('a').isNull()
    >>> eval_first_fail(df, 'a', expectation)
    (True, 2, {'a': 1})

    >>> spark.stop()

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
    df = df.select(column).filter(~expectation)
    if not df.isEmpty():
        first_failed_row = cast(Row, df.first())
        count_cases = df.count()
        return True, count_cases, first_failed_row.asDict()
    return False, 0, {}
