from __future__ import annotations

import re
from typing import Any

import pytest
from pyspark.sql import DataFrame

from src.sparkchecker.ext._decorators import (
    add_class_prefix,
    check_column_exist,
    check_dataframe,
    check_inputs,
    order_expectations_dict,
    validate_expectation,
)


class TestValidateExpectation:
    def test_validate_expectation_valid(self):
        @validate_expectation
        def sample_func(valid=True):
            if valid:
                return {"has_failed": False, "got": "some_value", "message": "All good"}
            return {"has_failed": True, "got": "some_value", "message": "Something went wrong"}
        result = sample_func(valid=True)
        assert result == {"has_failed": False, "got": "some_value", "message": "All good"}

    def test_validate_expectation_invalid(self):
        def sample_func(valid=True):
            if valid:
                return {"has_failed": False, "got": "some_value", "message": "All good"}
            return {"has_failed": True, "got": "some_value", "message": "Something went wrong"}
        result = sample_func(valid=False)
        assert result == {"has_failed": True, "got": "some_value", "message": "Something went wrong"}

    def test_validate_expectation_invalid_return_type(self):
        @validate_expectation
        def invalid_return_type_func():
            return "invalid_return_type"

        with pytest.raises(TypeError, match=re.escape("Expected return type 'dict', but got 'str'.")):
            invalid_return_type_func()

    def test_validate_expectation_missing_keys(self):
        @validate_expectation
        def missing_keys_func():
            return {"has_failed": False, "got": "some_value"}

        with pytest.raises(KeyError, match=re.escape("Invalid keys in return value. Expected one of: ['got, has_failed, message', 'example, got, has_failed, message'], but got: got, has_failed.")):
            missing_keys_func()

    def test_validate_expectation_extra_keys(self):
        @validate_expectation
        def extra_keys_func():
            return {"has_failed": False, "got": "some_value", "message": "All good", "extra_key": "extra_value"}

        with pytest.raises(KeyError, match=re.escape("Invalid keys in return value. Expected one of: ['got, has_failed, message', 'example, got, has_failed, message'], but got: extra_key, got, has_failed, message")):
            extra_keys_func()

    def test_validate_expectation_not_dict(self):
        @validate_expectation
        def extra_keys_func():
            return 1

        with pytest.raises(TypeError, match=re.escape("Expected return type 'dict', but got 'int'")):
            extra_keys_func()


class TestOrderExpectationsDict:
    def test_order_expectations_dict_valid(self):
        @order_expectations_dict
        def sample_func():
            return {
                "message": "test",
                "got": "value",
                "has_failed": False,
                "check": "check_value",
                "strategy": "strategy_value",
                "value": "some_value",
                "operator": "eq",
            }

        result = sample_func()
        expected_order = ["check", "has_failed", "strategy", "value", "got", "operator", "message"]
        assert list(result.keys()) == expected_order

    def test_order_expectations_dict_with_additional_keys(self):
        @order_expectations_dict
        def sample_func():
            return {
                "message": "test",
                "got": "value",
                "has_failed": False,
                "extra_key": "extra",
                "another_key": "another",
            }

        result = sample_func()
        # Check that predefined keys come first in correct order
        ordered_keys = list(result.keys())
        assert ordered_keys[:3] == ["has_failed", "got", "message"]
        # Check that additional keys are present
        assert "extra_key" in ordered_keys[3:]
        assert "another_key" in ordered_keys[3:]

    def test_order_expectations_dict_invalid_return_type(self):
        @order_expectations_dict
        def invalid_return_type_func():
            return "invalid_return_type"

        with pytest.raises(TypeError, match=re.escape("Expected return type 'dict', but got 'str'.")):
            invalid_return_type_func()

    def test_order_expectations_dict_empty_dict(self):
        @order_expectations_dict
        def empty_dict_func():
            return {}

        result = empty_dict_func()
        assert result == {}

    def test_order_expectations_dict_subset_keys(self):
        @order_expectations_dict
        def subset_keys_func():
            return {
                "message": "test",
                "has_failed": False,
            }

        result = subset_keys_func()
        assert list(result.keys()) == ["has_failed", "message"]


class TestCheckInputs:
    class SampleClass:
        @check_inputs
        def method_with_type_hints(self, x: int, y: str) -> None:
            pass

        @check_inputs
        def method_with_union(self, x: int | str) -> None:
            pass

        @check_inputs
        def method_with_any(self, x: Any) -> None:
            pass

        @check_inputs
        def method_without_hints(self, x) -> None:
            pass

        @check_inputs
        def method_with_default(self, x: int = 10) -> None:
            pass

    def setup_method(self):
        self.test_instance = self.SampleClass()

    def test_valid_input_types(self):
        # Should not raise any exception
        self.test_instance.method_with_type_hints(42, "test")

    def test_invalid_input_types(self):
        with pytest.raises(TypeError, match=re.escape("SampleClass: the argument `x` does not correspond to the expected types '[int]'. Got: str")):
            self.test_instance.method_with_type_hints("wrong", "test")

    def test_union_types(self):
        # Both types should work
        self.test_instance.method_with_union(42)
        self.test_instance.method_with_union("test")

    def test_any_type_hint(self):
        # Any type should work
        self.test_instance.method_with_any(42)
        self.test_instance.method_with_any("test")
        self.test_instance.method_with_any([1, 2, 3])

    def test_no_type_hints(self):
        # Should not raise any exception
        self.test_instance.method_without_hints("anything")

    def test_default_arguments(self):
        # Should work with both default and provided values
        self.test_instance.method_with_default()
        self.test_instance.method_with_default(20)

        with pytest.raises(TypeError, match=re.escape("SampleClass: the argument `x` does not correspond to the expected types '[int]'. Got: str")):
            self.test_instance.method_with_default("wrong")


class TestAddClassPrefix:
    class SampleClass:
        def __init__(self):
            self.message = "Original message"

        @add_class_prefix
        def sample_method(self):
            return True

    class AnotherClass:
        def __init__(self):
            self.message = "Another message"

        @add_class_prefix
        def sample_method(self):
            return True

    def test_basic_prefix_addition(self):
        instance = self.SampleClass()
        instance.sample_method()
        assert instance.message == "SampleClass: Original message"

    def test_multiple_calls(self):
        instance = self.SampleClass()
        instance.sample_method()
        instance.sample_method()
        assert instance.message == "SampleClass: Original message"

    def test_different_class_names(self):
        instance1 = self.SampleClass()
        instance2 = self.AnotherClass()

        instance1.sample_method()
        instance2.sample_method()

        assert instance1.message == "SampleClass: Original message"
        assert instance2.message == "AnotherClass: Another message"

    def test_empty_message(self):
        instance = self.SampleClass()
        instance.message = ""
        instance.sample_method()
        assert instance.message == "SampleClass: "

    def test_return_value_preserved(self):
        instance = self.SampleClass()
        result = instance.sample_method()
        assert result is True


class TestCheckDataFrame:
    class ColumnClass:
        def __init__(self):
            self.name = "test_column"

        @check_dataframe
        def method(self, df: DataFrame) -> dict:
            return {"result": "success"}

    class OtherClass:
        @check_dataframe
        def method(self, df: DataFrame) -> dict:
            return {"result": "success"}

    def test_valid_dataframe(self, spark_session):
        test_df = spark_session.createDataFrame([(1,)], ["col1"])
        instance = self.OtherClass()

        result = instance.method(test_df)
        assert result == {"result": "success"}

    def test_invalid_input(self):
        instance = self.OtherClass()
        with pytest.raises(TypeError, match=re.escape("OtherClass: The target must be a Spark DataFrame, but got 'str'")):
            instance.method("not_a_dataframe")

    def test_empty_dataframe_other_class(self, spark_session):
        # Create empty DataFrame
        empty_df = spark_session.createDataFrame([(1,)], ["col1"]).filter("col1 = 0").cache()
        instance = self.OtherClass()

        result = instance.method(empty_df)
        assert result == {"result": "success"}

    def test_return_value_preserved(self, spark_session):
        test_df = spark_session.createDataFrame([(1,)], ["col1"])
        instance = self.OtherClass()
        result = instance.method(test_df)
        assert result == {"result": "success"}


class TestCheckColumnExist:
    class ClassMock:
        def __init__(self, column):
            self.column = column

        @check_column_exist
        def test_method(self, df: DataFrame) -> str:
            return "success"

    def test_column_exists(self, spark_session):
        df = spark_session.createDataFrame([(1,)], ["test_col"])
        instance = self.ClassMock("test_col")
        result = instance.test_method(df)
        assert result == "success"

    def test_column_not_exists(self, spark_session):
        df = spark_session.createDataFrame([(1,)], ["existing_col"])
        instance = self.ClassMock("missing_col")
        with pytest.raises(ValueError, match=re.escape("ClassMock: Column 'missing_col' does not exist in the DataFrame")):
            instance.test_method(df)

    def test_case_sensitive(self, spark_session):
        df = spark_session.createDataFrame([(1,)], ["Test_Col"])
        instance = self.ClassMock("test_col")
        with pytest.raises(ValueError, match="ClassMock: Column 'test_col' does not exist in the DataFrame"):
            instance.test_method(df)

    def test_return_value_preserved(self, spark_session):
        df = spark_session.createDataFrame([(1,)], ["test_col"])
        instance = self.ClassMock("test_col")
        result = instance.test_method(df)
        assert result == "success"
