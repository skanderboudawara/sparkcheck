from typing import Any

from sparkchecker.constants import (
    COLUMN_OPERATIONS,
    COLUMN_TYPES,
    CONSTRAINT_CONSTRUCTOR,
    OPERATOR_MAP,
)


class SparkCheckerError(Exception):
    """
    A unified exception class for handling errors in Spark checks.

    This class consolidates multiple exception types for better maintainability.
    """

    INTERNAL_ERROR = "InternalError"
    ILLEGAL_CONSTRAINT_CONSTRUCTOR = "IllegalConstraintConstructor"
    ILLEGAL_HAS_COLUMN_EXPECTATIONS = "IllegalHasColumnExpectations"
    ILLEGAL_THRESHOLD_MATH_OPERATOR = "IllegalThresholdMathOperator"
    CONSTRAINTS_OUT_OF_RANGE = "ConstraintsOutOfRange"
    ILLEGAL_COLUMN_TYPE = "IllegalColumnType"
    ILLEGAL_COLUMN_CHECK = "IllegalColumnCheck"

    def __init__(
        self,
        error_type: str,
        constraint: str | None = None,
        exception: Any | str = None,
    ) -> None:
        """
        Initialize a SparkCheckerError with a specific error type and details.

        :param error_type: The type of error (e.g., INTERNAL_ERROR, ILLEGAL_CONSTRAINT_CONSTRUCTOR).
        :param constraint: The related constraint or context for the error.
        :param exception: The offending value or additional details about the error.
        """
        message = self._generate_message(error_type, constraint, exception)
        super().__init__(message)

    @staticmethod
    def _generate_message(
        error_type: str,
        constraint: str | None,
        exception: Any | str,
    ) -> str:
        """
        Generate an error message based on the error type and details.

        :param error_type: The type of error.
        :param constraint: The related constraint or context.
        :param exception: The offending value or additional details.
        :return: A formatted error message.
        """
        match error_type:
            case SparkCheckerError.INTERNAL_ERROR:
                return f"Expected: {constraint}, Got: {exception}"
            case SparkCheckerError.ILLEGAL_CONSTRAINT_CONSTRUCTOR:
                return (
                    f"Each constraint in `{constraint}` must have keys in "
                    f"\n{', '.join(CONSTRAINT_CONSTRUCTOR)}"
                    f"\nGot: {exception}"
                )
            case SparkCheckerError.ILLEGAL_HAS_COLUMN_EXPECTATIONS:
                return (
                    f"Constraint object must be a string in has_columns but got: "
                    f"{type(exception)!r} = {exception!r}"
                )
            case SparkCheckerError.ILLEGAL_THRESHOLD_MATH_OPERATOR:
                return (
                    f"`{constraint}` only takes these values as constraints "
                    f"\n{', '.join(OPERATOR_MAP.keys())}"
                    f"\nGot: {exception}"
                )
            case SparkCheckerError.CONSTRAINTS_OUT_OF_RANGE:
                return (
                    f"Each constraint in `{constraint}` must have 1 set of rules"
                    "\nAn Example:"
                    "\nlower:"
                    "\n    value: 10"
                    "\n    strategy: 'fail'"
                    "\n    message: 'lower failed'"
                    f"In your YAML file, we got {len(exception)} rules: {exception}"
                )
            case SparkCheckerError.ILLEGAL_COLUMN_TYPE:
                return (
                    f"`has_column` checks on `{constraint}` only take these values as types "
                    f"\n{', '.join(COLUMN_TYPES.keys())}"
                    f"\nGot: {exception}"
                )
            case SparkCheckerError.ILLEGAL_COLUMN_CHECK:
                authorized_values = [*OPERATOR_MAP, *COLUMN_OPERATIONS]
                return (
                    f"`{constraint}` only takes these values as constraints "
                    f"\n{', '.join(authorized_values)}"
                    f"\nGot: {exception}"
                )
            case _:
                return "An unknown error occurred."
