"""
File used for prototyping: src/proto.py.

Will be removed in the final version.
"""
from sparkchecker import (
    COLUMN_TYPES,
    CONSTRAINT_CONSTRUCTOR,
    OPERATOR_MAP,
    IllegalColumnType,
    IllegalConstraintConstructor,
    IllegalThresholdMathOperator,
    InternalError,
    parse_decimal_type,
    read_yaml_file,
)


class ConstraintConstructor:
    """
    Class used to construct constraints.
    """

    def __init__(self) -> None:
        """
        This class is used to construct constraints.

        :param None:

        :return: None
        """
        self.stack = []
        self._constraint = None
        self._constraint_obj = None

    @property
    def constraint(self) -> dict:
        """
        This method is a getter for the constraint attribute.

        :param None:

        :return: (None | Any), the constraint attribute
        """
        return self._constraint

    @property
    def constraint_obj(self) -> dict:
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
        else:
            raise InternalError({"item": {"constraint": 10}}, new_constraint)

    def check_constraint_objects(self) -> None:
        """
        This method checks the constraint objects.

        :param None:

        :return: None
        """
        for predicate in self._constraint_obj:
            if predicate not in CONSTRAINT_CONSTRUCTOR:
                raise IllegalConstraintConstructor(self.constraint, predicate)

    def check_threshold_items(self) -> None:
        """
        This method checks the threshold items.

        :param None:

        :return: None
        """
        if stack.constraint not in OPERATOR_MAP:
            raise IllegalThresholdMathOperator(self.constraint, self._constraint_obj)

    def check_and_dispatch_has_column(self) -> None:
        """
        This method checks and dispatches the has column.

        :param None:

        :return: None
        """
        for column in self._constraint_obj:
            if isinstance(column, dict):
                column_name, column_type = next(iter(column.items()))
                if column_type not in COLUMN_TYPES:
                    raise IllegalColumnType(column_name, column_type)

                column_type = (
                    parse_decimal_type(column_type)
                    if "decimal" in column_type
                    else COLUMN_TYPES[column_type]
                )
                constraint = {"column_name": column_name, "constraint": column_type}
            elif isinstance(column, str):
                constraint = {"column_name": column}
            else:
                raise ValueError("Parsing error")
            self.append("exist", constraint)

    def append(self, chk: str, constraint: dict) -> None:
        """
        This method appends the constraint.

        :param chk: (str), the check to append

        :param constraint: (dict), the constraint to append

        :return: None
        """
        constraint["check"] = chk
        self.stack.append(constraint)

    @property
    def stacks(self) -> list:
        """
        This method is a getter for the stack attribute.

        :param None:

        :return: (list), the stack attribute
        """
        return self.stack


yaml_checks = read_yaml_file(
    "/Users/skanderboudawara/Desktop/GitHub/sparkcheck/examples/check_df.yaml",
)

stack = ConstraintConstructor()
for item_to_check in yaml_checks:
    if item_to_check in {"count", "partitions"} and (
        constraints := yaml_checks.get(item_to_check)
    ):
        for constraint in constraints:
            stack.constraint = constraint
            stack.check_threshold_items()
            stack.check_constraint_objects()
            stack.append(item_to_check, stack.constraint_obj)
        continue
    if item_to_check in {"is_empty"} and (constraint := yaml_checks.get(item_to_check)):
        stack.constraint = {"is_empty": constraint}
        stack.check_constraint_objects()
        stack.append(item_to_check, stack.constraint_obj)
        continue
    if item_to_check in {"has_columns"} and (
        constraint := yaml_checks.get(item_to_check)
    ):
        stack.constraint = constraint
        stack.check_and_dispatch_has_column()
        continue
    if item_to_check in {"checks"} and (columns := yaml_checks.get(item_to_check)):
        for column in columns:
            column_name, constraints = next(iter(column.items()))
            for constraint in constraints:
                stack.constraint = constraint
                if stack.constraint in OPERATOR_MAP:
                    stack.check_threshold_items()
                    stack.check_constraint_objects()
        continue
