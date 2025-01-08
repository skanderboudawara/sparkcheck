"""
This module contains the ExpectationsYamlParser class.

This class is used to construct constraints.
"""

from typing import Optional, Union

from jsonpath_ng import parse

from sparkchecker.bin._constants import (
    COLUMN_OPERATIONS,
    COLUMN_TYPES,
    CONSTRAINT_CONSTRUCTOR,
    OPERATOR_MAP,
)
from sparkchecker.bin._exceptions import (
    IllegalColumnCheck,
    IllegalColumnType,
    IllegalConstraintConstructor,
    IllegalThresholdMathOperator,
    InternalError,
)
from sparkchecker.bin._utils import (
    parse_decimal_type,
)


class ExpectationsYamlParser:
    """
    Class used to construct constraints.
    """

    def __init__(self, yaml_data: dict) -> None:
        """
        This class is used to construct constraints.

        :param None:

        :return: None
        """
        self.data = yaml_data
        self.stack: list[dict] = []
        self._constraint: Union[str, None] = None
        self._constraint_obj: Union[dict, str, None] = None

    @property
    def constraint(self) -> str:
        """
        This method is a getter for the constraint attribute.

        :param None:

        :return: (None | Any), the constraint attribute
        """
        return self._constraint

    @property
    def constraint_obj(self) -> Union[dict, str, None]:
        """
        This method is a getter for the constraint_obj attribute.

        :param None:

        :return: (dict), the constraint_obj attribute
        """
        return self._constraint_obj

    @constraint.setter
    def constraint(self, new_constraint: dict) -> None:
        """
        This method is a setter for the constraint attribute.

        :param new_constraint: (dict), the new constraint to set

        :return: None
        """
        if isinstance(new_constraint, dict):
            self._constraint, self._constraint_obj = next(iter(new_constraint.items()))
        elif isinstance(new_constraint, str):
            self._constraint = new_constraint
            self._constraint_obj = None
        else:
            raise InternalError({"item": {"constraint": 10}}, new_constraint)

    def _verify_constructor_parsing(self) -> None:
        """
        This method checks the constraint objects.

        :param None:

        :return: None
        """
        if self._constraint_obj is None:
            raise IllegalConstraintConstructor(self.constraint, self._constraint_obj)
        for expectation in self._constraint_obj:
            if expectation not in CONSTRAINT_CONSTRUCTOR:
                raise IllegalConstraintConstructor(self.constraint, expectation)

    def _verify_threshold_parsing(self) -> None:
        """
        This method checks the threshold items.

        :param None:

        :return: None
        """
        if self.constraint not in OPERATOR_MAP:
            raise IllegalThresholdMathOperator(self.constraint, self._constraint_obj)

    def _verify_column_checks_parsing(self) -> None:
        if self.constraint not in {*OPERATOR_MAP, *COLUMN_OPERATIONS}:
            raise IllegalColumnCheck(self.constraint, self._constraint_obj)

    def _check_count(self) -> None:
        """
        This method checks the count.

        :param None:

        :return: None
        """
        count = parse("$.count")
        count = count.find(self.data)[0].value[0]
        if count:
            self.constraint = count
            self._verify_constructor_parsing()
            self._verify_threshold_parsing()
            self.append("count", self.constraint_obj, self.constraint)

    def _check_partitions(self) -> None:
        """
        This method checks the count.

        :param None:

        :return: None
        """
        partitions = parse("$.partitions")
        partitions = partitions.find(self.data)[0].value[0]
        if partitions:
            self.constraint = partitions
            self._verify_constructor_parsing()
            self._verify_threshold_parsing()
            self.append("partitions", self.constraint_obj, self.constraint)

    def _check_is_empty(self) -> None:
        """
        This method checks the is empty.

        :param None:

        :return: None
        """
        is_empty = parse("$.is_empty")
        is_empty = is_empty.find(self.data)[0].value
        if is_empty:
            self.constraint = {"is_empty": is_empty}
            self._verify_constructor_parsing()
            self.append("is_empty", self.constraint_obj)

    def _check_has_columns(self) -> None:
        """
        This method checks and dispatches the has column.

        :param None:

        :return: None
        """
        has_columns = parse("$.has_columns[*]")
        has_columns = has_columns.find(self.data)
        for column in has_columns:
            self.constraint = column.value
            if self._constraint_obj:
                if self._constraint_obj not in COLUMN_TYPES:
                    raise IllegalColumnType(self._constraint, self._constraint_obj)

                self._constraint_obj = (
                    parse_decimal_type(self._constraint_obj)
                    if "decimal" in self._constraint_obj
                    else COLUMN_TYPES[self._constraint_obj]
                )
                constraint = {
                    "column": self._constraint,
                    "constraint": self._constraint_obj,
                }
            else:
                constraint = {"column": self._constraint}
            self.append("exist", constraint)

    def _column_checks(self) -> None:
        """
        This method checks the constraints.

        :param None:

        :return: None
        """
        column_check = parse("$.checks[*]")
        column_check = column_check.find(self.data)
        for column in column_check:
            column_name, list_of_checks = next(iter(column.value.items()))
            for check in list_of_checks:
                self.constraint = check
                self._verify_column_checks_parsing()
                self._verify_constructor_parsing()
                self._constraint_obj.update({"column": column_name})
                self.append("column", self.constraint_obj, self.constraint)

    def parse(self) -> None:
        """
        This method runs the checks.

        :param None:

        :return: None
        """
        self._check_count()
        self._check_partitions()
        self._check_is_empty()
        self._check_has_columns()
        self._column_checks()

    def append(
        self,
        chk: Union[str, None],
        constraint: Union[dict, str, None],
        operator: Optional[str] = None,
    ) -> None:
        """
        This method appends the constraint.

        :param chk: (str), the check to append

        :param constraint: (dict), the constraint to append

        :return: None
        """
        if chk is None:
            raise ValueError("Check cannot be None")
        if constraint is None:
            raise ValueError("Constraint cannot be None")
        constraint["check"] = chk
        if operator:
            constraint["operator"] = operator
        self.stack.append(constraint)

    @property
    def stacks(self) -> list:
        """
        This method is a getter for the stack attribute.

        :param None:

        :return: (list), the stack attribute
        """
        return self.stack
