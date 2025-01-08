from functools import wraps


def validate_predicate(func):
    """
    A decorator to validate that the wrapped function returns a dictionary
    containing the keys: "predicate", "got", and "message".

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
        required_keys = {"predicate", "got", "message"}
        missing_keys = required_keys - result.keys()
        if missing_keys:
            raise KeyError(
                f"Missing required keys in return value: {', '.join(missing_keys)}",
            )

        return result

    return wrapper
