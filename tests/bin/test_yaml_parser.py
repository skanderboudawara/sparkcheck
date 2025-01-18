import re

import pytest
from pyspark.sql.types import DecimalType, StringType

from sparkchecker.bin._expectations_factory import COLUMN_CHECKS
from sparkchecker.bin._yaml_parser import (
    ExpectationsYamlParser,
    replace_keys_in_json,
)
from sparkchecker.constants import OPERATOR_MAP
from sparkchecker.ext._exceptions import SparkCheckerError


@pytest.fixture
def parser():
    return ExpectationsYamlParser({})


class TestSetConstraint:

    def test_set_constraint_with_valid_dict(self, parser):
        test_constraint = {"test_key": "test_value"}
        parser.set_constraint(test_constraint)
        assert parser._constraint == "test_key"
        assert parser._constraint_obj == "test_value"

    def test_set_constraint_with_string(self, parser):
        test_constraint = "test_constraint"
        parser.set_constraint(test_constraint)
        assert parser._constraint == "test_constraint"
        assert parser._constraint_obj is None

    @pytest.mark.parametrize("invalid_input", [
        10,
        ["list"],
        True,
        None,
        {"key1": "value1", "key2": "value2"},
        {},
    ])
    def test_set_constraint_with_invalid_type(self, parser, invalid_input):
        with pytest.raises(SparkCheckerError):
            parser.set_constraint(invalid_input)


class TestVerifyConstructorParsing:

    def test_valid_constraint(self, parser):
        parser.set_constraint({"higher": {"value": 0, "strategy": "warn"}})
        parser._verify_constructor_parsing()
        assert parser._constraint_obj == {"value": 0, "strategy": "warn"}
        assert parser._constraint == "higher"

    def test_none_constraint(self, parser):
        parser.set_constraint({"higher": None})
        with pytest.raises(ValueError, match="Constraint object cannot be None"):
            parser._verify_constructor_parsing()

    def test_non_dict_constraint(self, parser):
        parser.set_constraint({"higher": 1})
        with pytest.raises(TypeError, match="Expected a dict for constraint_obj"):
            parser._verify_constructor_parsing()

    def test_unknown_expectation(self, parser):
        parser.set_constraint({"unknown_expectation": {"value": 9}})
        with pytest.raises(SparkCheckerError):
            parser._verify_constructor_parsing()

    def test_invalid_message_type(self, parser):
        parser.set_constraint({"higher": {"value": 0, "strategy": "warn", "message": 1}})
        with pytest.raises(TypeError, match=re.escape("Message must be of type str but got: ', <class 'int'>")):
            parser._verify_constructor_parsing()

    def test_invalid_strategy_value(self, parser):
        parser.set_constraint({"higher": {"value": 0, "strategy": "wrong"}})
        with pytest.raises(ValueError, match="higher: strategy must be one of 'fail' or 'warn'but got: wrong"):
            parser._verify_constructor_parsing()

    def test_invalid_strategy_type(self, parser):
        parser.set_constraint({"higher": {"value": 0, "strategy": 1}})
        with pytest.raises(TypeError, match=re.escape("'higher: Strategy must be of type str but got: ', <class 'int'>")):
            parser._verify_constructor_parsing()

    @pytest.mark.parametrize("valid_strategy", ["fail", "warn"])
    def test_valid_strategies(self, parser, valid_strategy):
        parser.set_constraint({"higher": {"value": 0, "strategy": valid_strategy}})
        parser._verify_constructor_parsing()

    def test_invalid_constraint_obj(self, parser):
        parser.set_constraint({"higher": {"delta": 0, "strategy": "warn"}})
        with pytest.raises(SparkCheckerError):
            parser._verify_constructor_parsing()


class TestVerifyThresholdParsing:
    def test_valid_threshold(self, parser):
        parser.set_constraint({"higher": {"value": 0, "strategy": "warn"}})
        parser._verify_threshold_parsing()

    def test_invalid_constraint_type(self, parser):
        parser.set_constraint({1: {"value": 0, "strategy": "warn"}})
        with pytest.raises(ValueError, match="Constraint must be a string"):
            parser._verify_threshold_parsing()

    def test_invalid_constraint_obj_type(self, parser):
        parser.set_constraint({"higher": 1})
        with pytest.raises(ValueError, match="Constraint object must be a dict"):
            parser._verify_threshold_parsing()

    def test_invalid_operator(self, parser):
        parser.set_constraint({"slower": {"value": 0, "strategy": "warn"}})
        with pytest.raises(SparkCheckerError):
            parser._verify_threshold_parsing()

    @pytest.mark.parametrize("valid_operator", list(OPERATOR_MAP.keys()))
    def test_valid_operators(self, parser, valid_operator):
        parser.set_constraint({valid_operator: {"value": 0, "strategy": "warn"}})
        parser._verify_threshold_parsing()


class TestVerifyColumnChecksParsing:
    def test_valid_column_check(self, parser):
        parser.set_constraint({"higher": {"value": 0, "strategy": "warn"}})
        parser._verify_column_checks_parsing()

    def test_invalid_constraint_type(self, parser):
        parser.set_constraint({1: {"value": 0, "strategy": "warn"}})
        with pytest.raises(ValueError, match="Constraint must be a string"):
            parser._verify_column_checks_parsing()

    def test_invalid_constraint_obj_type(self, parser):
        parser.set_constraint({"higher": 1})
        with pytest.raises(ValueError, match="Constraint object must be a dict"):
            parser._verify_column_checks_parsing()

    def test_invalid_column_check(self, parser):
        parser.set_constraint({"slower": {"value": 0, "strategy": "warn"}})
        with pytest.raises(SparkCheckerError):
            parser._verify_column_checks_parsing()

    @pytest.mark.parametrize("valid_check", list(COLUMN_CHECKS.keys()))
    def test_valid_column_checks(self, parser, valid_check):
        parser.set_constraint({valid_check: {"value": 0, "strategy": "warn"}})
        parser._verify_column_checks_parsing()


class TestParserCheckCount:

    def test_count_exist(self, parser):
        parser.data = ({"count": [{"higher": {"value": 0, "strategy": "warn"}}]})
        parser._check_count()
        assert parser.stack == [{"value": 0, "strategy": "warn", "check": "count", "operator": "higher"}]

    def test_count_not_exist(self, parser):
        parser.data = ({"not_count": [{"higher": {"value": 0, "strategy": "warn"}}]})
        parser._check_count()
        assert parser.stack == []


class TestParserCheckPartitions:

    def test_partitions_exist(self, parser):
        parser.data = ({"partitions": [{"higher": {"value": 0, "strategy": "warn"}}]})
        parser._check_partitions()
        assert parser.stack == [{"value": 0, "strategy": "warn", "check": "partitions", "operator": "higher"}]

    def test_partitions_not_exist(self, parser):
        parser.data = ({"not_partitions": [{"higher": {"value": 0, "strategy": "warn"}}]})
        parser._check_partitions()
        assert parser.stack == []


class TestParserCheckIsEmpty:

    def test_is_empty_exist(self, parser):
        parser.data = ({"is_empty": {"value": False, "strategy": "fail"}})
        parser._check_is_empty()
        assert parser.stack == [{"value": False, "strategy": "fail", "check": "is_empty"}]

    def test_is_empty_not_exist(self, parser):
        parser.data = ({"not_is_empty": {"value": False, "strategy": "fail"}})
        parser._check_is_empty()
        assert parser.stack == []


class TestParserCheckHasColumns:

    def test_has_columns_exist(self, parser):
        parser.data = ({"has_columns": [{"passengers_name": "string"}, "passengers_age", {"passengers_net": "decimal(10,2)"}]})
        parser._check_has_columns()
        assert parser.stack == [
            {"column": "passengers_name", "value": StringType(), "check": "has_columns"},
            {"column": "passengers_age", "check": "has_columns"},
            {"column": "passengers_net", "value": DecimalType(10, 2), "check": "has_columns"},
        ]

    def test_has_columns_unknown_type(self, parser):
        parser.data = ({"has_columns": [{"passengers_name": "not_string"}]})
        with pytest.raises(SparkCheckerError):
            parser._check_has_columns()

    def test_has_columns_type_not_in_str(self, parser):
        parser.data = ({"has_columns": [{"passengers_name": 1}]})
        with pytest.raises(SparkCheckerError):
            parser._check_has_columns()

    def test_has_columns_not_exist(self, parser):
        parser.data = ({"not_has_columns": [{"passengers_name": "string"}, "'passengers_age'"]})
        parser._check_has_columns()
        assert parser.stack == []


class TestParserColumnChecks:

    def test_checks_exist(self, parser):
        data = {
            "checks": [
                {
                    "is_military": [
                        {"equal": {"value": False, "strategy": "fail", "message": "There should be no military in civilian dataset"}},
                        {"is_null": {"value": False}},
                    ],
                 },
                {
                    "passengers_country": [
                        {"different": {"value": "arrival_country", "strategy": "warn", "message": "The passenger country should be different from the arrival country in this dataset"}},
                        {"in": {"value": {"value": ["USA", "UK", "FR", "DE", "IT", "ES", "JP", "CN", "RU"], "strategy": "fail"}, "strategy": "fail"}},
                        {"is_null": {"value": False}},
                    ],
                },
            ],
        }
        parser.data = data
        parser._column_checks()
        assert parser.stack == [
            {"value": False, "strategy": "fail", "message": "There should be no military in civilian dataset", "column": "is_military", "check": "column", "operator": "equal"},
            {"value": False, "column": "is_military", "check": "column", "operator": "is_null"},
            {"value": "arrival_country", "strategy": "warn", "message": "The passenger country should be different from the arrival country in this dataset", "column": "passengers_country", "check": "column", "operator": "different"},
            {"value": {"value": ["USA", "UK", "FR", "DE", "IT", "ES", "JP", "CN", "RU"], "strategy": "fail"}, "strategy": "fail", "column": "passengers_country", "check": "column", "operator": "in"},
            {"value": False, "column": "passengers_country", "check": "column", "operator": "is_null"},
        ]

    def test_checks_not_exist(self, parser):
        parser.data = ({"not_checks": [{"passengers_name": "string"}, "'passengers_age'"]})
        parser._check_has_columns()
        assert parser.stack == []


class TestParserAppend:
    @pytest.fixture
    def valid_constraint(self):
        return {"value": 10}

    def test_valid_append(self, parser, valid_constraint):
        parser.append("test_check", valid_constraint)
        assert parser.stack == [{"value": 10, "check": "test_check"}]

    def test_append_with_operator(self, parser, valid_constraint):
        parser.append("test_check", valid_constraint, "eq")
        assert parser.stack == [{"value": 10, "check": "test_check", "operator": "eq"}]

    @pytest.mark.parametrize(("chk", "constraint", "operator", "error_msg"), [
        (None, {"value": 10}, None, "Check cannot be None"),
        ("test", None, None, "Constraint cannot be None"),
        (123, {"value": 10}, None, "Check must be a string"),
        ("test", "not_a_dict", None, "Constraint must be a dictionary"),
    ])
    def test_invalid_inputs(self, parser, chk, constraint, operator, error_msg):
        with pytest.raises(ValueError, match=re.escape(error_msg)):
            parser.append(chk, constraint, operator)


class TestReplaceKeysInJson:
    @pytest.fixture
    def basic_replacements(self):
        return {"old_key": "new_key"}

    def test_basic_replacement(self, basic_replacements):
        input_data = {"old_key": "value"}
        expected = {"new_key": "value"}
        result = replace_keys_in_json(input_data, basic_replacements)
        assert result == expected

    def test_nested_dict_replacement(self, basic_replacements):
        input_data = {
            "level1": {
                "old_key": "value",
            },
        }
        expected = {
            "level1": {
                "new_key": "value",
            },
        }
        result = replace_keys_in_json(input_data, basic_replacements)
        assert result == expected

    def test_list_in_dict(self, basic_replacements):
        input_data = {
            "items": [
                {"old_key": "value1"},
                {"old_key": "value2"},
            ],
        }
        expected = {
            "items": [
                {"new_key": "value1"},
                {"new_key": "value2"},
            ],
        }
        result = replace_keys_in_json(input_data, basic_replacements)
        assert result == expected

    def test_mixed_data_structure(self, basic_replacements):
        input_data = {
            "old_key": [
                {"old_key": "value"},
                ["nested", {"old_key": "value"}],
            ],
        }
        expected = {
            "new_key": [
                {"new_key": "value"},
                ["nested", {"new_key": "value"}],
            ],
        }
        result = replace_keys_in_json(input_data, basic_replacements)
        assert result == expected

    def test_empty_inputs(self):
        assert replace_keys_in_json({}, {}) == {}
        assert replace_keys_in_json([], {}) == []

    def test_non_existing_replacement(self):
        input_data = {"keep_key": "value"}
        replacements = {"non_existing": "new"}
        result = replace_keys_in_json(input_data, replacements)
        assert result == input_data

    def test_multiple_replacements(self):
        input_data = {
            "key1": "value1",
            "key2": "value2",
        }
        replacements = {
            "key1": "new1",
            "key2": "new2",
        }
        expected = {
            "new1": "value1",
            "new2": "value2",
        }
        result = replace_keys_in_json(input_data, replacements)
        assert result == expected
