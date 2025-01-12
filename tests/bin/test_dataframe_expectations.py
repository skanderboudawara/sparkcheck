import pytest
import re
from abc import ABC, abstractmethod
from src.sparkchecker.bin._dataframe_expectations import (
    DfIsEmptyCheck,
    DfCountThresholdCheck,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    BooleanType,
    DateType,
    TimestampType,
)
from pyspark.sql.functions import col, lit
from datetime import datetime


@pytest.fixture
def df_test(spark_session):
    schema = StructType(
        [
            StructField("name"         , StringType()   , True),
            StructField("age"          , IntegerType()  , True),
            StructField("height"       , DoubleType()   , True),
            StructField("is_student"   , BooleanType()  , True),
            StructField("birth_date"   , DateType()     , True),
            StructField("last_check_in", TimestampType(), True),
        ]
    )

    data = [
    #   (name     , age, height, is_student, birth_date          , last_check_in             ),  # noqa
        ("Alice"  , 25 , 1.60  , True      , datetime(1996, 1, 1), datetime(2021, 1, 1, 0, 0)),
        ("Bob"    , 30 , 1.75  , True      , datetime(1991, 1, 1), datetime(2021, 1, 1, 0, 0)),
        ("Charlie", 35 , 1.80  , True      , datetime(1986, 1, 1), datetime(2021, 1, 1, 0, 0)),
    ]

    df = spark_session.createDataFrame(data, schema)
    return df

@pytest.fixture
def df_test_empty(spark_session):
    schema = StructType(
        [
            StructField("name", StringType(), True),
        ]
    )

    df = spark_session.createDataFrame([], schema)
    return df


class TestDfIsEmptyCheck:

    @pytest.mark.parametrize(
        "value, message",
        [
            (True, None),
            (False, None),
            (True, "hello world"),
        ],
    )
    def test_init(self, value, message):
        DfIsEmptyCheck(value, message)

    @pytest.mark.parametrize(
        "value, message, exception, match",
        [
            (True, 1, TypeError, re.escape("DfIsEmptyCheck: the argument `message` does not correspond to the expected types '[str | NoneType]'. Got: int")),
            ("1", None, TypeError, re.escape("DfIsEmptyCheck: the argument `value` does not correspond to the expected types '[bool | NoneType]'. Got: str")),
        ],
    )
    def test_init_exceptions(
        self, value, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            DfIsEmptyCheck(value, message)

    @pytest.mark.parametrize(
        "custom_message, has_failed, expected_message",
        [
            ("custom message", True, "DfIsEmptyCheck: custom message"),
            ("custom message", False, "DfIsEmptyCheck: custom message"),
            (None, True, "DfIsEmptyCheck: The DataFrame is not empty"),
            (None, False, "DfIsEmptyCheck: The DataFrame is empty"),
        ],
    )
    def test_get_message(
        self,
        custom_message,
        has_failed,
        expected_message,
    ):
        expectations = DfIsEmptyCheck(True, custom_message)
        expectations.get_message(has_failed)
        assert expectations.message == expected_message

    @pytest.mark.parametrize(
        "value, custom_message, is_empty, expected_result",
        [
            (True, None, False,
                {
                    "got": True,
                    "has_failed": True,
                    "message": "DfIsEmptyCheck: The DataFrame is not empty",
                },
            ),
            (True, None, True,
                {
                    "got": False,
                    "has_failed": False,
                    "message": "DfIsEmptyCheck: The DataFrame is empty",
                },
            ),
            (False, None, False,
                {
                    "got": False,
                    "has_failed": False,
                    "message": "DfIsEmptyCheck: The DataFrame is not empty",
                },
            ),
            (False, None, True,
                {
                    "got": True,
                    "has_failed": True,
                    "message": "DfIsEmptyCheck: The DataFrame is empty",
                },
            ),
        ],
    )
    def test_eval_expectation(
        self,
        df_test,
        df_test_empty,
        value,
        custom_message,
        is_empty,
        expected_result,
    ):
        df = df_test_empty if is_empty else df_test
        expectations = DfIsEmptyCheck(value, custom_message)
        assert expectations.eval_expectation(df) == expected_result

    def test_eval_expectation_exception(self):
        expectations = DfIsEmptyCheck(True, "name")
        with pytest.raises(TypeError, match="DfIsEmptyCheck: The target must be a Spark DataFrame, but got 'int'"):
            expectations.eval_expectation(1)


class TestDfCountThresholdCheck:

    @pytest.mark.parametrize(
        "value, operator, message",
        [
            (1, "lower", None),
            (1, "lower", "hello world"),
        ],
    )
    def test_init(self, value, operator, message):
        DfCountThresholdCheck(value, operator, message)

    @pytest.mark.parametrize(
        "value, operator, message, exception, match",
        [
            (1, "lower", 1, TypeError, re.escape("DfCountThresholdCheck: the argument `message` does not correspond to the expected types '[str | NoneType]'. Got: int")),
            ("1", "lower", None, TypeError, re.escape("DfCountThresholdCheck: the argument `value` does not correspond to the expected types '[int]'. Got: str")),
            (2, "flower", "1", ValueError, re.escape("DfCountThresholdCheck: Invalid operator: 'flower'. Must be one of: '[lower, lower_or_equal, equal, different, higher, higher_or_equal]")),
        ],
    )
    def test_init_exceptions(
        self, value, operator, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            DfCountThresholdCheck(value, operator, message)

    @pytest.mark.parametrize(
        "custom_message, has_failed, expected_message",
        [
            ("custom message", True, "DfCountThresholdCheck: custom message"),
            ("custom message", False, "DfCountThresholdCheck: custom message"),
            (None, True, "DfCountThresholdCheck: The DataFrame has 10 rows, which is not lower than 1"),
            (None, False, "DfCountThresholdCheck: The DataFrame has 10 rows, which is lower than 1"),
        ],
    )
    def test_get_message(
        self,
        custom_message,
        has_failed,
        expected_message,
    ):
        expectations = DfCountThresholdCheck(1, "lower", custom_message)
        expectations.result = 10
        expectations.get_message(has_failed)
        assert expectations.message == expected_message

    @pytest.mark.parametrize(
        "value, operator, custom_message, is_empty, expected_result",
        [
            (1, "higher", None, False,
                {
                    "got": 3,
                    "has_failed": False,
                    "message": "DfCountThresholdCheck: The DataFrame has 3 rows, which is higher than 1",
                },
            ),
            (1, "lower", None, False,
                {
                    "got": 3,
                    "has_failed": True,
                    "message": "DfCountThresholdCheck: The DataFrame has 3 rows, which is not lower than 1",
                },
            ),
            (3, "equal", None, False,
                {
                    "got": 3,
                    "has_failed": False,
                    "message": "DfCountThresholdCheck: The DataFrame has 3 rows, which is equal to 3",
                },
            ),
            (2, "different", None, False,
                {
                    "got": 3,
                    "has_failed": False,
                    "message": "DfCountThresholdCheck: The DataFrame has 3 rows, which is different to 2",
                },
            ),
            (0, "equal", None, True,
                {
                    "got": 0,
                    "has_failed": False,
                    "message": "DfCountThresholdCheck: The DataFrame has 0 rows, which is equal to 0",
                },
            ),
            (1, "higher", None, True,
                {
                    "got": 0,
                    "has_failed": True,
                    "message": "DfCountThresholdCheck: The DataFrame has 0 rows, which is not higher than 1",
                },
            ),
        ],
    )
    def test_eval_expectation(
        self,
        df_test,
        df_test_empty,
        value,
        operator,
        custom_message,
        is_empty,
        expected_result,
    ):
        df = df_test_empty if is_empty else df_test
        expectations = DfCountThresholdCheck(value, operator, custom_message)
        assert expectations.eval_expectation(df) == expected_result

    def test_eval_expectation_exception(self):
        expectations = DfCountThresholdCheck(1, "lower", "name")
        with pytest.raises(TypeError, match="DfCountThresholdCheck: The target must be a Spark DataFrame, but got 'int'"):
            expectations.eval_expectation(1)
