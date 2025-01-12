"""
This module contains the ExpectationsYamlParser class.

This class is used to construct constraints.
"""

import yaml

from sparkchecker.constants import (
    COLUMN_OPERATIONS,
    COLUMN_TYPES,
    CONSTRAINT_CONSTRUCTOR,
    OPERATOR_MAP,
)
from sparkchecker.ext._exceptions import SparkCheckerError
from sparkchecker.ext._utils import (
    to_decimal,
)


class ExpectationsYamlParser:
    """
    Class used to construct constraints.
    """

    def __init__(self, yaml_data: dict) -> None:
        """
        This class is used to construct constraints.

        :param yaml_data: (dict), the yaml data to parse
        :return: None
        """
        self.data = yaml_data
        self.stack: list[dict] = []
        self._constraint: str | None = None
        self._constraint_obj: dict | str | None = None

    @property
    def constraint(self) -> str | None:
        """
        This method is a getter for the constraint attribute.

        :param: None
        :return: (None | Any), the constraint attribute
        """
        return self._constraint

    @property
    def constraint_obj(self) -> dict | str | None:
        """
        This method is a getter for the constraint_obj attribute.

        :param: None
        :return: (dict), the constraint_obj attribute
        """
        return self._constraint_obj

    def set_constraint(self, new_constraint: dict) -> None:
        """
        This method is a setter for the constraint attribute.

        :param new_constraint: (dict), the new constraint to set
        :return: None
        :raises: (SparkCheckerError), if the constraint is not a dict
        """
        if isinstance(new_constraint, dict):
            self._constraint, self._constraint_obj = next(
                iter(new_constraint.items()),
            )
        elif isinstance(new_constraint, str):
            self._constraint = new_constraint
            self._constraint_obj = None
        else:
            raise SparkCheckerError(
                SparkCheckerError.InternalError,
                {"item": {"constraint": 10}},
                new_constraint,
            )

    def _verify_constructor_parsing(self) -> None:
        """
        This method checks the constraint objects.

        :param: None
        :return: None
        :raises: (SparkCheckerError), if the expectation is not
            in the CONSTRAINT_CONSTRUCTOR
        """
        if self.constraint_obj is None:
            raise ValueError("Constraint object cannot be None")
        if not isinstance(self.constraint_obj, dict):
            raise TypeError(
                "Expected a dict for constraint_obj, "
                f"but got: {type(self.constraint_obj)}",
            )
        for expectation in self.constraint_obj:
            if expectation not in CONSTRAINT_CONSTRUCTOR:
                raise SparkCheckerError(
                    SparkCheckerError.ILLEGAL_CONSTRAINT_CONSTRUCTOR,
                    self.constraint,
                    expectation,
                )
        if (message := self.constraint_obj.get("message")) and not isinstance(
            message,
            str,
        ):
            raise TypeError(
                "Message must be of type str but got: ",
                type(message),
            )
        if (
            strategy := self.constraint_obj.get("strategy")
        ) and not isinstance(
            strategy,
            str,
        ):
            raise TypeError(
                "Strategy must be of type str but got: ",
                type(strategy),
            )
        if (
            strategy := self.constraint_obj.get("strategy")
        ) and strategy not in {
            "fail",
            "warn",
        }:
            raise ValueError(
                "Strategy must be one of 'fail' or 'warn' but got: "
                f"{strategy}",
            )

    def _verify_threshold_parsing(self) -> None:
        """
        This method checks the threshold items.

        :param: None
        :return: None
        :raises: (ValueError), if the constraint is not a string
        :raises: (ValueError), if the constraint object is not a dict
        :raises: (SparkCheckerError), if the expectation is
            not in the OPERATOR_MAP
        """
        if not isinstance(self.constraint, str):
            raise ValueError("Constraint must be a string")
        if not isinstance(self.constraint_obj, dict):
            raise ValueError("Constraint object must be a dict")
        if self.constraint not in OPERATOR_MAP:
            raise SparkCheckerError(
                SparkCheckerError.ILLEGAL_THRESHOLD_MATH_OPERATOR,
                self.constraint,
                self.constraint_obj,
            )

    def _verify_column_checks_parsing(self) -> None:
        """
        This method checks the column checks.

        :param: None
        :return: None
        :raises: (ValueError), if the constraint is not a string
        :raises: (ValueError), if the constraint object is not a dict
        :raises: (SparkCheckerError), if the expectation
            is not in the OPERATOR_MAP and COLUMN_OPERATIONS
        """
        if not isinstance(self.constraint, str):
            raise ValueError("Constraint must be a string")
        if not isinstance(self.constraint_obj, dict):
            raise ValueError("Constraint object must be a dict")
        if self.constraint not in {*OPERATOR_MAP, *COLUMN_OPERATIONS}:
            raise SparkCheckerError(
                SparkCheckerError.ILLEGAL_COLUMN_CHECK,
                self.constraint,
                self.constraint_obj,
            )

    def _check_count(self) -> None:
        """
        This method checks the count.

        :param: None
        :return: None
        """
        count: dict = self.data.get("count", [{}])[0]
        if count:
            self.set_constraint(count)
            self._verify_constructor_parsing()
            self._verify_threshold_parsing()
            self.append("count", self.constraint_obj, self.constraint)

    def _check_partitions(self) -> None:
        """
        This method checks the count.

        :param: None
        :return: None
        """
        partitions: dict = self.data.get("partitions", [{}])[0]
        if partitions:
            self.set_constraint(partitions)
            self._verify_constructor_parsing()
            self._verify_threshold_parsing()
            self.append("partitions", self.constraint_obj, self.constraint)

    def _check_is_empty(self) -> None:
        """
        This method checks the is empty.

        :param: None
        :return: None
        """
        is_empty: dict = self.data.get("is_empty", {})
        if is_empty:
            self.set_constraint({"is_empty": is_empty})
            self._verify_constructor_parsing()
            self.append("is_empty", self.constraint_obj)

    def _check_has_columns(self) -> None:
        """
        This method checks and dispatches the has column.

        :param: None
        :return: None
        :raises: (SparkCheckerError), if the constraint object is not a string
        :raises: (SparkCheckerError), if the constraint object
            is not in COLUMN_TYPES
        """
        has_columns: list = self.data.get("has_columns", [])
        for column in has_columns:
            self.set_constraint(column)
            if self.constraint_obj:
                if not isinstance(self.constraint_obj, str):
                    raise SparkCheckerError(
                        SparkCheckerError.ILLEGAL_HAS_COLUMN_EXPECTATIONS,
                        repr(self.constraint_obj),
                    )
                if self.constraint_obj not in COLUMN_TYPES:
                    raise SparkCheckerError(
                        SparkCheckerError.ILLEGAL_COLUMN_TYPE,
                        self.constraint,
                        self.constraint_obj,
                    )
                column_type = (
                    to_decimal(self.constraint_obj)
                    if "decimal" in self.constraint_obj
                    else COLUMN_TYPES[self.constraint_obj]
                )
                constraint = {
                    "column": self.constraint,
                    "value": column_type,
                }
            else:
                constraint = {"column": self.constraint}
            self.append("has_columns", constraint)

    def _column_checks(self) -> None:
        """
        This method checks the constraints.

        :param: None
        :return: None
        :raises: (ValueError), if the constraint object is not a dict
        """
        column_check = self.data.get("checks", {})
        for column in column_check:
            column_name, list_of_checks = next(iter(column.items()))
            for check in list_of_checks:
                self.set_constraint(check)
                self._verify_column_checks_parsing()
                self._verify_constructor_parsing()
                if not isinstance(self.constraint_obj, dict):
                    raise ValueError("Constraint object must be a dict")
                self.constraint_obj.update({"column": column_name})
                self.append("column", self.constraint_obj, self.constraint)

    def parse(self) -> None:
        """
        This method runs the checks.

        :param: None
        :return: None
        """
        self._check_count()
        self._check_partitions()
        self._check_is_empty()
        self._check_has_columns()
        self._column_checks()

    def append(
        self,
        chk: str | None,
        constraint: dict | str | None,
        operator: str | None = None,
    ) -> None:
        """
        This method appends the constraint.

        :param chk: (str), the check to append
        :param constraint: (dict), the constraint to append
        :param operator: (str), the operator to append
        :return: None
        :raises: (ValueError), if the check is None
        :raises: (ValueError), if the constraint is None
        :raises: (ValueError), if the check is not a string
        :raises: (ValueError), if the constraint is not a dictionary
        """
        if chk is None:
            raise ValueError("Check cannot be None")
        if constraint is None:
            raise ValueError("Constraint cannot be None")
        if not isinstance(chk, str):
            raise ValueError("Check must be a string")
        if not isinstance(constraint, dict):
            raise ValueError("Constraint must be a dictionary")
        constraint["check"] = chk
        if operator:
            constraint["operator"] = operator
        self.stack.append(constraint)

    @property
    def stacks(self) -> list:
        """
        This method is a getter for the stack attribute.

        :param: None
        :return: (list), the stack attribute
        """
        return self.stack


def read_yaml_file(file_path: str) -> dict:
    """
    Reads a YAML file and returns the parsed data.

    :param file_path: Path to the YAML file.
    :return: (dict) Parsed data as a Python dictionary.
    """
    with open(file_path, encoding="utf-8") as file:
        data = yaml.safe_load(file)
    return data


def replace_keys_in_json(json_data: dict, replacements: dict) -> dict:
    """
    Replaces specified keys in a JSON-like dictionary with
        new keys based on a mapping.

    :param json_data (dict): The input JSON-like dictionary.
    :param replacements (dict): A dictionary mapping old keys to new keys.
    :returns: (dict)n The modified dictionary with keys replaced.
    """
    if isinstance(json_data, dict):
        # Replace keys in the current dictionary
        return {
            replacements.get(key, key): replace_keys_in_json(
                value,
                replacements,
            )
            for key, value in json_data.items()
        }
    if isinstance(json_data, list):
        # Recursively process each item in the list
        return [replace_keys_in_json(item, replacements) for item in json_data]
    # Return the value as is if it's neither a dict nor a list
    return json_data
