from typing import Any, Union

from sparkchecker.bin._constants import (
    COLUMN_OPERATIONS,
    COLUMN_TYPES,
    CONSTRAINT_CONSTRUCTOR,
    OPERATOR_MAP,
)


class InternalError(Exception):
    def __init__(self, constraint: str, exception: Union[dict, str]) -> None:
        """
        This class raises an exception when an internal error occurs.

        :param constraint: (str), the constraint

        :param exception: (str), the exception

        :return: None
        """
        message = f"Expected: {constraint}, Got: {exception}"
        super().__init__(message)


class IllegalConstraintConstructor(Exception):
    def __init__(self, constraint: Union[str, None], exception: dict) -> None:
        """
        This class raises an exception when an illegal constraint constructor is used.

        :param constraint: (str), the constraint

        :param exception: (str), the exception

        :return: None
        """
        message = f"Each constraint in `{constraint}` must have keys in \
                        {', '.join(CONSTRAINT_CONSTRUCTOR)} \ngot: {exception}"
        super().__init__(message)


class IllegalHasColumnExpectations(Exception):
    def __init__(self, exception: Any) -> None:
        """
        This class raises an exception when an illegal constraint constructor is used.

        :param constraint: (str), the constraint

        :param exception: (str), the exception

        :return: None
        """
        message = " ".join(
            [
                "Constraint object must be a string in has_columns but got: ",
                repr(type(exception)),
                " = ",
                repr(exception),
            ],
        )
        super().__init__(message)


class IllegalThresholdMathOperator(Exception):
    def __init__(self, constraint: Union[str, None], exception: dict) -> None:
        """
        This class raises an exception when an illegal threshold math operator is used.

        :param constraint: (str), the constraint

        :param exception: (str), the exception

        :return: None
        """
        message = (
            f"`{constraint}` only takes these values as constraints \
                    {', '.join(OPERATOR_MAP.keys())} \ngot: {exception}",
        )
        super().__init__(message)


class ConstrainsOutOfRange(Exception):
    def __init__(
        self,
        constraint: Union[str, None],
        exception: Union[dict, str],
    ) -> None:
        """
        This class raises an exception when the constraints are out of range.

        :param constraint: (str), the constraint

        :param exception: (str), the exception

        :return: None
        """
        message = " ".join(
            [
                f"Each constraint in `{constraint}` must have 1 set of rules",
                "\nAn Example:",
                "\nlower:",
                "\n    constraint: 10",
                "\n    strategy: 'fail'",
                "\n    message: 'lower failed'",
                f"In your YAML file we got {len(exception)} rules : {exception}",
            ],
        )
        super().__init__(message)


class IllegalColumnType(Exception):
    def __init__(self, constraint: Union[str, None], exception: str) -> None:
        """
        This class raises an exception when an illegal column type is used.

        :param constraint: (str), the constraint

        :param exception: (str), the exception
        """
        # The base Exception class accepts a message argument
        message = (
            f"`has_column` checks on `{constraint}` only takes these values as types \
                    {', '.join(COLUMN_TYPES.keys())} \ngot: {exception}",
        )
        super().__init__(message)


class IllegalColumnCheck(Exception):
    def __init__(self, constraint: Union[str, None], exception: dict) -> None:
        """
        This class raises an exception when an illegal threshold math operator is used.

        :param constraint: (str), the constraint

        :param exception: (str), the exception

        :return: None
        """
        list_of_authorized_values = [*OPERATOR_MAP, *COLUMN_OPERATIONS]
        message = (
            f"`{constraint}` only takes these values as constraints \
                    {', '.join(list_of_authorized_values)} \ngot: {exception}",
        )
        super().__init__(message)


class IllegalCheckStrategy(Exception):
    def __init__(self, check: dict) -> None:
        """
        This class raises an exception when an illegal threshold math operator is used.

        :param constraint: (str), the constraint

        :param exception: (str), the exception

        :return: None
        """
        chk = check["check"]
        strategy = check["strategy"]
        message = (
            f"for check {chk} the strategy must be one of 'fail' or 'warn', got: {strategy}",
        )
        super().__init__(message)
