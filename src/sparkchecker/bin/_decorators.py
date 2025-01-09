import inspect
from collections.abc import Callable
from functools import wraps
from typing import Any

from pyspark.sql import DataFrame

from sparkchecker.bin._utils import col_to_name


def validate_expectation(func: Callable) -> Callable:
    """
    A decorator to validate that the wrapped function returns a dictionary
    containing the keys: "expectation", "got", and "message".

    :param func (callable): The function to wrap.

    :returns callable: The wrapped function with validation.
    """

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> dict:
        result = func(*args, **kwargs)

        # Check if the return value is a dictionary
        if not isinstance(result, dict):
            raise TypeError(
                f"Expected return type 'dict', but got '{type(result).__name__}'.",
            )
        max_dict_df = 3
        max_dict_cols = 4
        if len(result.keys()) not in {max_dict_df, max_dict_cols}:
            raise ValueError(
                f"Expected {max_dict_df} or {max_dict_cols} keys in return value.",
            )
        missing_keys = None
        if len(result.keys()) == max_dict_cols:
            required_keys = {"has_failed", "got", "message", "example"}
            missing_keys = required_keys - result.keys()
        elif len(result.keys()) == max_dict_df:
            required_keys = {"has_failed", "got", "message"}
            missing_keys = required_keys - result.keys()
        if missing_keys:
            raise KeyError(
                f"Missing required keys in return value: {', '.join(missing_keys)}",
            )

        return result

    return wrapper


def order_expectations_dict(func: Callable) -> Callable:
    """
    A decorator to validate that the wrapped function returns a dictionary
    containing the keys: "expectation", "got", and "message".

    :param func (callable): The function to wrap.

    :returns callable: The wrapped function with validation.
    """

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> dict:
        result = func(*args, **kwargs)

        key_order = [
            "check",
            "has_failed",
            "strategy",
            "value",
            "got",
            "operator",
            "message",
        ]
        ordered_dict = {key: result[key] for key in key_order if key in result}
        additional_keys = {key: result[key] for key in result if key not in key_order}
        ordered_dict.update(additional_keys)
        return ordered_dict

    return wrapper


def check_column_exist(method: Callable) -> Callable:
    def _expectation(self: Any, df: DataFrame) -> Any:
        column_name = col_to_name(self.column)
        if column_name not in df.columns:
            raise ValueError(f"Column {column_name} does not exist in DataFrame")
        return method(self, df)

    _expectation.__name__ = method.__name__
    _expectation.__doc__ = method.__doc__
    return _expectation


def check_message(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        # Check if 'message' is in args or kwargs
        message = None
        sig = inspect.signature(func)
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()
        if "message" in bound_args.arguments:
            message = bound_args.arguments["message"]

        # Validate 'message' is a string
        if message is not None and not isinstance(message, str):
            raise TypeError(
                f"Expected 'message' to be of type 'str', but got '{type(message).__name__}'.",
            )

        return func(*args, **kwargs)

    return wrapper
