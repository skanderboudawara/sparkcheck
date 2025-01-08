import inspect
from functools import wraps

from sparkchecker.bin._utils import col_to_name


def validate_expectation(func):
    """
    A decorator to validate that the wrapped function returns a dictionary
    containing the keys: "expectation", "got", and "message".

    :param func (callable): The function to wrap.

    :returns callable: The wrapped function with validation.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)

        # Check if the return value is a dictionary
        if not isinstance(result, dict):
            raise TypeError(
                f"Expected return type 'dict', but got '{type(result).__name__}'.",
            )

        # Check if required keys are present
        required_keys = {"expectation", "got", "message"}
        missing_keys = required_keys - result.keys()
        if missing_keys:
            raise KeyError(
                f"Missing required keys in return value: {', '.join(missing_keys)}",
            )

        return result

    return wrapper


def check_column_exist(method):
    def _expectation(
        self,
    ):
        column_name = col_to_name(self.column)
        if column_name not in self.df.columns:
            raise ValueError(f"Column {column_name} does not exist in DataFrame")
        return method(self)

    _expectation.__name__ = method.__name__
    _expectation.__doc__ = method.__doc__
    return _expectation


def check_message(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
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
