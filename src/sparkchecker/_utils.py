from typing import Union

from pyspark.sql import Column
from pyspark.sql.functions import col, lit

from sparkchecker._constants import OPERATOR_MAP


def _str_to_col(column_name: Union[str, Column], is_lit: bool = False) -> Column:
    """
    Convert a `column_name` string to a column.

    :param column_name: (Union[str, Column]), `pyspark.sql.Column` or a column name

    :returns: (Column) a spark Column
    """
    if isinstance(column_name, str):
        return lit(column_name) if is_lit else col(column_name)
    if isinstance(column_name, Column):
        return column_name
    raise TypeError(
        "Argument `column_name` must be a string or a Column but got: ",
        type(column_name),
    )


def _args_to_list_cols(
    list_args: Union[
        str,
        Column,
        list[Column],
        list[str],
        tuple[str, ...],
        tuple[Column, ...],
    ],
    is_lit: bool = False,
) -> list[Column]:
    """
    Convert a `list_args` to a list[Columns].

    :param list_args: (Union[str, Column, list[Column], list[str], tuple[str, ...], tuple[Column, ...]]),
        a list of arguments

    :returns: (list[Column]), a list of Column
    """
    if isinstance(list_args, (str, Column)):
        return list(_str_to_col(list_args, is_lit))
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
    return [_str_to_col(arg, is_lit) for arg in list_args]


def _check_operator(operator: str) -> None:
    if operator not in OPERATOR_MAP:
        raise ValueError(
            f"Invalid operator: {operator}. Must be one of {list(OPERATOR_MAP.keys())}",
        )
