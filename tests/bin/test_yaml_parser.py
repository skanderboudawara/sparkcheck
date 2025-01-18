import re

import pytest

from sparkchecker.bin._yaml_parser import ExpectationsYamlParser
from sparkchecker.ext._exceptions import SparkCheckerError


class TestSetConstraint:
    @pytest.fixture
    def parser(self):
        return ExpectationsYamlParser({})

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
    @pytest.fixture
    def parser(self):
        return ExpectationsYamlParser({})

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

    def test_invalid_strategy_type(self, parser):
        parser.set_constraint({"higher": {"value": 0, "strategy": "warn", "message": 1}})
        with pytest.raises(TypeError, match=re.escape("Message must be of type str but got: ', <class 'int'>")):
            parser._verify_constructor_parsing()

    def test_invalid_strategy_value(self, parser):
        parser.set_constraint({"higher": {"value": 0, "strategy": "wrong"}})
        with pytest.raises(ValueError, match="higher: strategy must be one of 'fail' or 'warn'but got: wrong"):
            parser._verify_constructor_parsing()

    @pytest.mark.parametrize("valid_strategy", ["fail", "warn"])
    def test_valid_strategies(self, parser, valid_strategy):
        parser.set_constraint({"higher": {"value": 0, "strategy": valid_strategy}})
        parser._verify_constructor_parsing()  # Should not raise
