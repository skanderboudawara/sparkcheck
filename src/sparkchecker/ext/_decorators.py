import inspect
from collections.abc import Callable
from functools import wraps
from typing import Any

from pyspark.sql import DataFrame

from sparkchecker.ext._utils import to_name


def validate_expectation(func: Callable) -> Callable:
    """
    A decorator to validate that the wrapped function returns a dictionary
    with the required structure.

    The dictionary must contain either:
    - Keys: "has_failed", "got", and "message" (3 keys).
    - Or Keys: "has_failed", "got", "message", and "example" (4 keys).

    :param func: (Callable), The function to wrap.
    :return: (Callable), The wrapped function with validation.
    :raises: (TypeError), If the return value is not a dictionary.
    :raises: (KeyError), If the dictionary does not have the required keys.
    """

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> dict[str, Any]:
        result = func(*args, **kwargs)

        if not isinstance(result, dict):
            raise TypeError(
                "Expected return type 'dict', "
                f"but got '{type(result).__name__}'.",
            )

        valid_key_sets = [
            {"has_failed", "got", "message"},
            {"has_failed", "got", "message", "example"},
        ]

        result_keys = set(result.keys())

        if result_keys not in valid_key_sets:
            expected_keys = [
                ", ".join(sorted(keys)) for keys in valid_key_sets
            ]
            raise KeyError(
                "Invalid keys in return value. "
                f"Expected one of: {expected_keys}, "
                f"but got: {', '.join(sorted(result_keys))}.",
            )

        return result

    return wrapper


def order_expectations_dict(func: Callable) -> Callable:
    """
    A decorator to order the keys of the dictionary returned by
        the wrapped function.

    The dictionary will be ordered based on a predefined key order.
        Any additional keys not in the predefined order will
        be appended at the end in their original order.

    :param func: (Callable), The function to wrap.
    :return:(Callable), The wrapped function with ordered dictionary keys.
    :raises: (TypeError), If the return value is not a dictionary.
    """

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> dict:
        result = func(*args, **kwargs)

        if not isinstance(result, dict):
            raise TypeError(
                "Expected return type 'dict', "
                f"but got '{type(result).__name__}'.",
            )

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
        additional_keys = {
            key: result[key] for key in result if key not in key_order
        }
        ordered_dict.update(additional_keys)
        return ordered_dict

    return wrapper


def check_column_exist(method: Callable) -> Callable:
    """
    A decorator to check if a specified column exists in the DataFrame.

    This decorator checks if the column specified in the 'self.column'
        attribute exists in the DataFrame passed to the decorated method.
        If the column does not exist, a ValueError is raised.

    :param method: (Callable), The method to be decorated.
    :return: (Callable), The wrapped method with column existence validation.
    :raises: (ValueError), If the specified column does not exist in
        the DataFrame.
    """

    @wraps(method)
    def _expectation(self: Any, target: DataFrame) -> Any:
        column_name = to_name(self.column)
        if column_name not in target.columns:
            target.printSchema()
            raise ValueError(
                f"Column '{column_name}' does not exist in the DataFrame",
            )
        return method(self, target)

    return _expectation


def check_message(func: Callable) -> Callable:
    """
    A decorator that checks if the 'message' argument passed to the decorated
        function is a string.

    This decorator inspects the arguments passed to the decorated function
        and ensures that if a 'message' argument is present, it is of
        type 'str'. If 'message' is not a string, a TypeError is raised.

    :param func: (Callable), The function to be decorated.
    :return: (Callable), The wrapped function with 'message' type validation.
    :raises: (TypeError), If 'message' is present and is not of type 'str'.
    """

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        # Check if 'message' is in args or kwargs
        sig = inspect.signature(func)
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()
        message = bound_args.arguments.get("message")

        # Validate 'message' is a string
        if message is not None and not isinstance(message, str):
            raise TypeError(
                "Expected 'message' to be of type 'str', "
                f"but got '{type(message).__name__}'.",
            )

        return func(*args, **kwargs)

    return wrapper
