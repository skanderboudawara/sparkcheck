import os
import re

import pytest
from pyspark.sql import Column
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType, IntegerType, StructField, StructType

from sparkchecker.constants import OPERATOR_MAP
from sparkchecker.ext._utils import (
    _op_check,
    _resolve_msg,
    _substitute,
    eval_first_fail,
    split_base_file,
    to_col,
    to_decimal,
    to_name,
)


class TestToCol:
    def test_string_as_column(self, spark_session):
        result = to_col("test_col", is_col=True)
        assert isinstance(result, Column)
        df = spark_session.createDataFrame([(1,)], ["test_col"])
        assert df.select(result).collect()[0][0] == 1

    def test_string_as_literal(self, spark_session):
        result = to_col("test_value", is_col=False)
        assert isinstance(result, Column)
        df = spark_session.createDataFrame([(1,)], ["dummy"])
        assert df.select(result).collect()[0][0] == "test_value"

    def test_string_escaped(self, spark_session):
        result = to_col("test\\value", is_col=False, escaped=True)
        assert isinstance(result, Column)
        df = spark_session.createDataFrame([(1,)], ["dummy"])
        assert df.select(result).collect()[0][0] == r"test\value"

    @pytest.mark.parametrize(("value", "expected_type"), [
        (42, int),
        (3.14, float),
        (True, bool),
    ])
    def test_numeric_inputs(self, spark_session, value, expected_type):
        result = to_col(value)
        assert isinstance(result, Column)
        df = spark_session.createDataFrame([(1,)], ["dummy"])
        assert isinstance(df.select(result).collect()[0][0], expected_type)

    def test_column_input(self, spark_session):
        input_col = col("test")
        result = to_col(input_col)
        assert result is input_col

    def test_none_input(self, spark_session):
        result = to_col(None)
        assert isinstance(result, Column)
        df = spark_session.createDataFrame([(1,)], ["dummy"])
        assert df.select(result).collect()[0][0] == "NoneObject"

    def test_invalid_input(self):
        with pytest.raises(TypeError, match="must be of type"):
            to_col([1, 2, 3])


class TestToName:
    def test_string_input(self, spark_session):
        result = to_name("test_column")
        assert result == "test_column"

    def test_column_input(self, spark_session):
        column = col("test_column")
        result = to_name(column)
        assert result == "test_column"

    def test_none_input(self, spark_session):
        result = to_name(None)
        assert result == "NoneObject"

    @pytest.mark.parametrize(("input_val", "expected"), [
        (42, "42"),
        (3.14, "3.14"),
        (True, "True"),
    ])
    def test_numeric_inputs(self, spark_session, input_val, expected):
        result = to_name(input_val)
        assert result == expected

    def test_invalid_input(self, spark_session):
        with pytest.raises(TypeError, match=re.escape("must be of type str | Column")):
            to_name([1, 2, 3])


class Test_OpCheck:
    class MockClass:
        def check_operator(self, operator):
            _op_check(self, operator)

    def setup_method(self):
        self.instance = self.MockClass()

    def test_valid_operators(self):
        for operator in OPERATOR_MAP:
            # Should not raise any exception
            self.instance.check_operator(operator)

    def test_invalid_operator(self):
        invalid_operator = "invalid_op"
        expected_operators = ", ".join(OPERATOR_MAP.keys())

        expected_error = (
            f"MockClass: Invalid operator: '{invalid_operator}'. "
            f"Must be one of: '[{expected_operators}]'"
        )

        with pytest.raises(ValueError, match=re.escape(expected_error)):
            self.instance.check_operator(invalid_operator)

    def test_empty_operator(self):
        with pytest.raises(ValueError, match="Invalid operator: ''"):
            self.instance.check_operator("")


class TestToDecimal:
    @pytest.mark.parametrize(("input_str", "expected"), [
        ("decimal(10,2)", DecimalType(10, 2)),
        ("decimal(5,0)", DecimalType(5, 0)),
        ("DECIMAL(38,18)", DecimalType(38, 18)),
        ("decimal(10, 2)", DecimalType(10, 2)),
        ("  decimal(7,3)  ", DecimalType(7, 3)),
    ])
    def test_valid_decimal_strings(self, spark_session, input_str, expected):
        result = to_decimal(input_str)
        assert isinstance(result, DecimalType)
        assert result.precision == expected.precision
        assert result.scale == expected.scale

    @pytest.mark.parametrize("invalid_input", [
        "decimal",
        "decimal()",
        "decimal(10)",
        "decimal(10,)",
        "decimal(,2)",
        "decimal(a,2)",
        "decimal(10,b)",
        "decimal(-1,2)",
        "decimal(10,-2)",
        "decimal(10.5,2)",
        "decimal 10,2",
        "(10,2)",
    ])
    def test_invalid_decimal_strings(self, spark_session, invalid_input):
        with pytest.raises(ValueError, match="Invalid decimal type string"):
            to_decimal(invalid_input)

    def test_type_checking(self, spark_session):
        result = to_decimal("decimal(10,2)")
        assert isinstance(result, DecimalType)


class TestSplitBaseFile:
    @pytest.mark.parametrize(("input_path", "expected_filename", "expected_path"), [
        (
            "test.py",
            "test",
            os.path.normpath("test_sparkchecker_result.log"),
        ),
        (
            "/path/to/file.txt",
            "file",
            os.path.normpath("/path/to/file_sparkchecker_result.log"),
        ),
        (
            "folder/test_file",
            "test_file",
            os.path.normpath("folder/test_file_sparkchecker_result.log"),
        ),
        (
            "path/file.name.with.dots.txt",
            "file.name.with.dots",
            os.path.normpath("path/file.name.with.dots_sparkchecker_result.log"),
        ),
        (
            os.path.join("directory", "subdirectory", "file.py"),
            "file",
            os.path.normpath(os.path.join("directory", "subdirectory", "file_sparkchecker_result.log")),
        ),
    ])
    def test_split_base_file(self, input_path, expected_filename, expected_path):
        filename, path = split_base_file(input_path)
        assert filename == expected_filename
        assert path == expected_path

    def test_empty_path(self):
        filename, path = split_base_file("")
        assert filename == ""  # noqa: PLC1901
        assert path == os.path.normpath("_sparkchecker_result.log")


class TestSubstitute:
    @pytest.mark.parametrize(("string", "condition", "placeholder", "expected"), [
        ("This <$is|not> a test", True, "<$is|not>", "This is a test"),
        ("This <$is|not> a test", False, "<$is|not>", "This not a test"),
        ("Value <$exists|missing>!", True, "<$exists|missing>", "Value exists!"),
        ("Value <$exists|missing>!", False, "<$exists|missing>", "Value missing!"),
        ("", True, "<$yes|no>", ""),  # Empty string test
        ("No placeholder text", True, "<$yes|no>", "No placeholder text"),  # No replacement needed
    ])
    def test_valid_substitutions(self, string, condition, placeholder, expected):
        result = _substitute(string, condition, placeholder)
        assert result == expected

    @pytest.mark.parametrize("invalid_input", [
        (123, True, "<$yes|no>"),
        ("text", "not_bool", "<$yes|no>"),
        ("text", True, 123),
    ])
    def test_invalid_types(self, invalid_input):
        with pytest.raises(TypeError):
            _substitute(*invalid_input)

    @pytest.mark.parametrize("invalid_placeholder", [
        "<yes|no>",
        "yes|no",
        "<$yes>",
    ])
    def test_invalid_placeholder_format(self, invalid_placeholder):
        with pytest.raises(ValueError, match="Invalid placeholder format"):
            _substitute("test", True, invalid_placeholder)

    def test_multiple_placeholders(self):
        input_str = "This <$is|not> a <$good|bad> test"
        result = _substitute(input_str, True, "<$is|not>")
        assert result == "This is a <$good|bad> test"


class TestResolveMsg:
    @pytest.mark.parametrize(("default", "msg", "expected"), [
        ("default", "override", "override"),
        ("default", None, "default"),
        ("", "override", "override"),
        ("default", "", ""),
        ("", None, ""),
    ])
    def test_valid_inputs(self, default, msg, expected):
        result = _resolve_msg(default, msg)
        assert result == expected

    @pytest.mark.parametrize(("default", "msg"), [
        (123, "message"),        # invalid default type
        (None, "message"),       # invalid default type
        ("default", 123),        # invalid msg type
        (["default"], None),     # invalid default type
        ("default", ["msg"]),    # invalid msg type
    ])
    def test_invalid_types(self, default, msg):
        with pytest.raises(TypeError):
            _resolve_msg(default, msg)


class TestEvalFirstFail:
    def test_valid_expectation_pass(self, spark_session):
        # Create test DataFrame where all rows pass
        df = spark_session.createDataFrame([(1,), (2,), (3,)], ["value"])
        failed, count, result = eval_first_fail(
            df,
            "value",
            col("value") > 0,
        )
        assert not failed
        assert count == 0
        assert result == {}

    def test_valid_expectation_fail(self, spark_session):
        # Create test DataFrame where some rows fail
        df = spark_session.createDataFrame([(1,), (-2,), (3,)], ["value"])
        failed, count, result = eval_first_fail(
            df,
            "value",
            col("value") > 0,
        )
        assert failed
        assert count == 1
        assert result == {"value": -2}

    def test_empty_dataframe(self, spark_session):
        schema = StructType([StructField("value", IntegerType())])
        df = spark_session.createDataFrame([], schema)
        df = df.cache()
        failed, count, result = eval_first_fail(
            df,
            "value",
            col("value") > 0,
        )
        assert not failed
        assert count == 0
        assert result == {}

    def test_column_as_string(self, spark_session):
        df = spark_session.createDataFrame([(1,)], ["test"])
        failed, count, result = eval_first_fail(
            df,
            "test",
            col("test") > 0,
        )
        assert not failed
        assert count == 0
        assert result == {}

    def test_column_as_column(self, spark_session):
        df = spark_session.createDataFrame([(1,)], ["test"])
        failed, count, result = eval_first_fail(
            df,
            col("test"),
            col("test") > 0,
        )
        assert not failed
        assert count == 0
        assert result == {}

    def test_invalid_inputs(self, spark_session):
        spark_session.createDataFrame([(1,)], ["test"])
        with pytest.raises(TypeError, match=re.escape("Argument `df` must be of type DataFrame but got: ', <class 'str'>")):
            eval_first_fail("not_a_dataframe", "col", col("col") > 0)
        with pytest.raises(TypeError, match=re.escape("Argument `df` must be of type DataFrame but got: ', <class 'NoneType'>")):
            eval_first_fail(None, "col", col("col") > 0)
        with pytest.raises(TypeError, match=re.escape("Argument `expectation` must be of type Column but got: ', <class 'str'>")):
            eval_first_fail("not_a_dataframe", "col", "not_a_column")
