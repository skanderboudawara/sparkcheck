import inspect
from collections.abc import Callable
from functools import wraps
from typing import Any, get_type_hints

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
        class_name = self.__class__.__name__
        column_name = to_name(self.column)
        if column_name not in target.columns:
            target.printSchema()
            raise ValueError(
                f"{class_name}: Column '{column_name}' does not exist "
                "in the DataFrame",
            )
        return method(self, target)

    return _expectation


def check_inputs(func: Callable) -> Callable:
    """
    This decorator checks if the arguments passed to the decorated function
        correspond to the expected types.

    The expected types are inferred from the type hints of the function.

    If an argument does not correspond to the expected type,
        a TypeError is raised.

    :param func: (Callable), The function to be decorated.
    :return: (Callable), The wrapped function with input type checking.
    :raises: (TypeError), If an argument does not correspond to
        the expected type.
    """

    @wraps(func)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        class_name = self.__class__.__name__

        # Get the function's signature and type hints
        signature = inspect.signature(func)
        type_hints = get_type_hints(func)

        # Bind the arguments to the function signature
        bound_args = signature.bind(self, *args, **kwargs)
        bound_args.apply_defaults()

        # Check each argument against its type hint
        for arg_name, arg_value in bound_args.arguments.items():
            if arg_name == "self":  # Skip `self`
                continue

            expected_type = type_hints.get(arg_name)
            if (
                expected_type is None or expected_type is Any
            ):  # Skip type checking for Any
                continue
            if expected_type and not isinstance(arg_value, expected_type):
                types_str = " | ".join(
                    t.__name__
                    for t in getattr(
                        expected_type,
                        "__args__",
                        [expected_type],
                    )
                )
                raise TypeError(
                    f"{class_name}: the argument `{arg_name}` does not "
                    f"correspond to the expected types '[{types_str}]'. "
                    f"Got: {type(arg_value).__name__}",
                )

        return func(self, *args, **kwargs)

    return wrapper


def add_class_prefix(func: Callable) -> Callable:
    """
    This decorator adds the class name as a prefix to the 'message' attribute
        of the class instance.

    :param func: (Callable), The function to be decorated.
    :return: (Callable), The wrapped function with the class name prefix.
    """

    @wraps(func)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        # Call the original function and get its return value
        result = func(self, *args, **kwargs)
        # Get the class name dynamically
        class_name = self.__class__.__name__
        self.message = f"{class_name}: {self.message}"
        return result

    return wrapper


def check_dataframe(method: Callable) -> Callable:
    """
    A decorator to check if the target is a Spark DataFrame and is not empty.

    :param method: (Callable), The method to be decorated.
    :return: (Callable), The wrapped method with DataFrame validation.
    :raises: (TypeError), If the target is not a Spark DataFrame
    """

    @wraps(method)
    def wrapper(self: Any, target: DataFrame) -> Any:
        class_name = self.__class__.__name__
        if not isinstance(target, DataFrame):
            raise TypeError(
                f"{class_name}: The target must be a Spark DataFrame, "
                f"but got '{type(target).__name__}'.",
            )
        if target.isEmpty():
            return {
                "has_failed": False,
                "got": "Empty DataFrame",
                "message": f"{class_name}: The DataFrame is empty.",
            }

        return method(self, target)

    return wrapper
