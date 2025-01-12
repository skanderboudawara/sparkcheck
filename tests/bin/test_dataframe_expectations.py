import pytest
import re
from abc import ABC, abstractmethod
from src.sparkchecker.bin._dataframe_expectations import (
    DataFrameIsEmptyCheck,
    DataFrameCountThresholdCheck,
    DataFramePartitionsCountCheck,
    DataFrameHasColumnsCheck,
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

class TestDfExpectation(ABC):

    @abstractmethod
    def test_init(self):
        pass

    @abstractmethod
    def test_init_exceptions(self):
        pass

    @abstractmethod
    def test_get_message(self):
        pass

    @abstractmethod
    def test_eval_expectation(self):
        pass

    @abstractmethod
    def test_eval_expectation_exception(self):
        pass

class TestDataFrameIsEmptyCheck(TestDfExpectation):

    @pytest.mark.parametrize(
        "value, message",
        [
            (True, None),
            (False, None),
            (True, "hello world"),
        ],
    )
    def test_init(self, value, message):
        DataFrameIsEmptyCheck(value, message)

    @pytest.mark.parametrize(
        "value, message, exception, match",
        [
            (True, 1, TypeError, re.escape("DataFrameIsEmptyCheck: the argument `message` does not correspond to the expected types '[str | NoneType]'. Got: int")),
            ("1", None, TypeError, re.escape("DataFrameIsEmptyCheck: the argument `value` does not correspond to the expected types '[bool | NoneType]'. Got: str")),
        ],
    )
    def test_init_exceptions(
        self, value, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            DataFrameIsEmptyCheck(value, message)

    @pytest.mark.parametrize(
        "custom_message, has_failed, expected_message",
        [
            ("custom message", True, "DataFrameIsEmptyCheck: custom message"),
            ("custom message", False, "DataFrameIsEmptyCheck: custom message"),
            (None, True, "DataFrameIsEmptyCheck: The DataFrame is not empty"),
            (None, False, "DataFrameIsEmptyCheck: The DataFrame is empty"),
        ],
    )
    def test_get_message(
        self,
        custom_message,
        has_failed,
        expected_message,
    ):
        expectations = DataFrameIsEmptyCheck(True, custom_message)
        expectations.get_message(has_failed)
        assert expectations.message == expected_message

    @pytest.mark.parametrize(
        "value, custom_message, is_empty, expected_result",
        [
            (True, None, False,
                {
                    "got": True,
                    "has_failed": True,
                    "message": "DataFrameIsEmptyCheck: The DataFrame is not empty",
                },
            ),
            (True, None, True,
                {
                    "got": False,
                    "has_failed": False,
                    "message": "DataFrameIsEmptyCheck: The DataFrame is empty",
                },
            ),
            (False, None, False,
                {
                    "got": False,
                    "has_failed": False,
                    "message": "DataFrameIsEmptyCheck: The DataFrame is not empty",
                },
            ),
            (False, None, True,
                {
                    "got": True,
                    "has_failed": True,
                    "message": "DataFrameIsEmptyCheck: The DataFrame is empty",
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
        expectations = DataFrameIsEmptyCheck(value, custom_message)
        assert expectations.eval_expectation(df) == expected_result

    def test_eval_expectation_exception(self):
        expectations = DataFrameIsEmptyCheck(True, "name")
        with pytest.raises(TypeError, match="DataFrameIsEmptyCheck: The target must be a Spark DataFrame, but got 'int'"):
            expectations.eval_expectation(1)

class TestDataFrameCountThresholdCheck(TestDfExpectation):

    @pytest.mark.parametrize(
        "value, operator, message",
        [
            (1, "lower", None),
            (1, "lower", "hello world"),
        ],
    )
    def test_init(self, value, operator, message):
        DataFrameCountThresholdCheck(value, operator, message)

    @pytest.mark.parametrize(
        "value, operator, message, exception, match",
        [
            (1, "lower", 1, TypeError, re.escape("DataFrameCountThresholdCheck: the argument `message` does not correspond to the expected types '[str | NoneType]'. Got: int")),
            ("1", "lower", None, TypeError, re.escape("DataFrameCountThresholdCheck: the argument `value` does not correspond to the expected types '[int]'. Got: str")),
            (2, "flower", "1", ValueError, re.escape("DataFrameCountThresholdCheck: Invalid operator: 'flower'. Must be one of: '[lower, lower_or_equal, equal, different, higher, higher_or_equal]")),
        ],
    )
    def test_init_exceptions(
        self, value, operator, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            DataFrameCountThresholdCheck(value, operator, message)

    @pytest.mark.parametrize(
        "custom_message, has_failed, expected_message",
        [
            ("custom message", True, "DataFrameCountThresholdCheck: custom message"),
            ("custom message", False, "DataFrameCountThresholdCheck: custom message"),
            (None, True, "DataFrameCountThresholdCheck: The DataFrame has 10 rows, which is not lower than 1"),
            (None, False, "DataFrameCountThresholdCheck: The DataFrame has 10 rows, which is lower than 1"),
        ],
    )
    def test_get_message(
        self,
        custom_message,
        has_failed,
        expected_message,
    ):
        expectations = DataFrameCountThresholdCheck(1, "lower", custom_message)
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
                    "message": "DataFrameCountThresholdCheck: The DataFrame has 3 rows, which is higher than 1",
                },
            ),
            (1, "lower", None, False,
                {
                    "got": 3,
                    "has_failed": True,
                    "message": "DataFrameCountThresholdCheck: The DataFrame has 3 rows, which is not lower than 1",
                },
            ),
            (3, "equal", None, False,
                {
                    "got": 3,
                    "has_failed": False,
                    "message": "DataFrameCountThresholdCheck: The DataFrame has 3 rows, which is equal to 3",
                },
            ),
            (2, "different", None, False,
                {
                    "got": 3,
                    "has_failed": False,
                    "message": "DataFrameCountThresholdCheck: The DataFrame has 3 rows, which is different to 2",
                },
            ),
            (0, "equal", None, True,
                {
                    "got": 0,
                    "has_failed": False,
                    "message": "DataFrameCountThresholdCheck: The DataFrame has 0 rows, which is equal to 0",
                },
            ),
            (1, "higher", None, True,
                {
                    "got": 0,
                    "has_failed": True,
                    "message": "DataFrameCountThresholdCheck: The DataFrame has 0 rows, which is not higher than 1",
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
        expectations = DataFrameCountThresholdCheck(value, operator, custom_message)
        assert expectations.eval_expectation(df) == expected_result

    def test_eval_expectation_exception(self):
        expectations = DataFrameCountThresholdCheck(1, "lower", "name")
        with pytest.raises(TypeError, match="DataFrameCountThresholdCheck: The target must be a Spark DataFrame, but got 'int'"):
            expectations.eval_expectation(1)

class TestDataFramePartitionsCountCheck(TestDfExpectation):

    @pytest.mark.parametrize(
        "value, operator, message",
        [
            (1, "lower", None),
            (1, "lower", "hello world"),
        ],
    )
    def test_init(self, value, operator, message):
        DataFramePartitionsCountCheck(value, operator, message)

    @pytest.mark.parametrize(
        "value, operator, message, exception, match",
        [
            (1, "lower", 1, TypeError, re.escape("DataFramePartitionsCountCheck: the argument `message` does not correspond to the expected types '[str | NoneType]'. Got: int")),
            ("1", "lower", None, TypeError, re.escape("DataFramePartitionsCountCheck: the argument `value` does not correspond to the expected types '[int]'. Got: str")),
            (2, "flower", "1", ValueError, re.escape("DataFramePartitionsCountCheck: Invalid operator: 'flower'. Must be one of: '[lower, lower_or_equal, equal, different, higher, higher_or_equal]")),
        ],
    )
    def test_init_exceptions(
        self, value, operator, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            DataFramePartitionsCountCheck(value, operator, message)

    @pytest.mark.parametrize(
        "custom_message, has_failed, expected_message",
        [
            ("custom message", True, "DataFramePartitionsCountCheck: custom message"),
            ("custom message", False, "DataFramePartitionsCountCheck: custom message"),
            (None, True, "DataFramePartitionsCountCheck: The DataFrame has 10 partitions, which is not lower than 1"),
            (None, False, "DataFramePartitionsCountCheck: The DataFrame has 10 partitions, which is lower than 1"),
        ],
    )
    def test_get_message(
        self,
        custom_message,
        has_failed,
        expected_message,
    ):
        expectations = DataFramePartitionsCountCheck(1, "lower", custom_message)
        expectations.result = 10
        expectations.get_message(has_failed)
        assert expectations.message == expected_message

    @pytest.mark.parametrize(
        "value, operator, custom_message, repartition, expected_result",
        [
            (1, "equal", None, 1,
                {
                    "got": 1,
                    "has_failed": False,
                    "message": "DataFramePartitionsCountCheck: The DataFrame has 1 partitions, which is equal to 1",
                },
            ),
            (1, "equal", None, 3,
                {
                    "got": 3,
                    "has_failed": True,
                    "message": "DataFramePartitionsCountCheck: The DataFrame has 3 partitions, which is not equal to 1",
                },
            ),
            (1, "equal", "custom_message", 1,
                {
                    "got": 1,
                    "has_failed": False,
                    "message": "DataFramePartitionsCountCheck: custom_message",
                },
            ),
            (1, "equal", "custom_message", 3,
                {
                    "got": 3,
                    "has_failed": True,
                    "message": "DataFramePartitionsCountCheck: custom_message",
                },
            ),
        ],
    )
    def test_eval_expectation(
        self,
        df_test,
        value,
        operator,
        custom_message,
        repartition,
        expected_result,
    ):
        df = df_test
        df = df.repartition(repartition) if repartition > 1 else df.coalesce(repartition)
        expectations = DataFramePartitionsCountCheck(value, operator, custom_message)
        assert expectations.eval_expectation(df) == expected_result

    def test_eval_expectation_exception(self):
        expectations = DataFramePartitionsCountCheck(1, "lower", "name")
        with pytest.raises(TypeError, match="DataFramePartitionsCountCheck: The target must be a Spark DataFrame, but got 'int'"):
            expectations.eval_expectation(1)

class TestDataFrameHasColumnsCheck(TestDfExpectation):

    @pytest.mark.parametrize(
        "column, value, message",
        [
            ("name", StringType(), None),
            ("name", StringType(), "hello world"),
            ("name", None, None),
            ("name", None, "hello world"),
        ],
    )
    def test_init(self, column, value, message):
        DataFrameHasColumnsCheck(column, value, message)

    @pytest.mark.parametrize(
        "column, value, message, exception, match",
        [
            ("name", StringType(), 1, TypeError, re.escape("DataFrameHasColumnsCheck: the argument `message` does not correspond to the expected types '[str | NoneType]'. Got: int")),
            (1, StringType(), None, TypeError, re.escape("DataFrameHasColumnsCheck: the argument `column` does not correspond to the expected types '[str]'. Got: int")),
            ("name", "string", "custom", TypeError, re.escape("DataFrameHasColumnsCheck: the argument `value` does not correspond to the expected types '[DataType | NoneType]'. Got: str")),
        ],
    )
    def test_init_exceptions(
        self, column, value, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            DataFrameHasColumnsCheck(column, value, message)

    @pytest.mark.parametrize(
        "value, custom_message, has_failed, expected_message",
        [
            (None, "custom message", True, "DataFrameHasColumnsCheck: custom message"),
            (None, "custom message", False, "DataFrameHasColumnsCheck: custom message"),
            (None, None, True, "DataFrameHasColumnsCheck: Column 'name' doesn't exist in the DataFrame"),
            (None, None, False, "DataFrameHasColumnsCheck: Column 'name' does exist in the DataFrame"),
            (StringType(), "custom message", True, "DataFrameHasColumnsCheck: custom message"),
            (StringType(), "custom message", False, "DataFrameHasColumnsCheck: custom message"),
            (StringType(), None, True, "DataFrameHasColumnsCheck: Column 'name' exists in the DataFrame but it's not of type: StringType()"),
            (StringType(), None, False, "DataFrameHasColumnsCheck: Column 'name' exists in the DataFrame and it's of type: StringType()"),
        ],
    )
    def test_get_message(
        self,
        value,
        custom_message,
        has_failed,
        expected_message,
    ):
        expectations = DataFrameHasColumnsCheck("name", value, custom_message)
        expectations.get_message(has_failed)
        assert expectations.message == expected_message

    @pytest.mark.parametrize(
        "column, value, custom_message, expected_result",
        [
            ("name", None, None,
                {
                    "got": "name",
                    "has_failed": False,
                    "message": "DataFrameHasColumnsCheck: Column 'name' does exist in the DataFrame",
                },
            ),
            ("not_name", None, None,
                {
                    "got": "name, age, height, is_student, birth_date, last_check_in",
                    "has_failed": True,
                    "message": "DataFrameHasColumnsCheck: Column 'not_name' doesn't exist in the DataFrame",
                },
            ),
            ("name", None, "custom_message",
                {
                    "got": "name",
                    "has_failed": False,
                    "message": "DataFrameHasColumnsCheck: custom_message",
                },
            ),
            ("not_name", None, "custom_message",
                {
                    "got": "name, age, height, is_student, birth_date, last_check_in",
                    "has_failed": True,
                    "message": "DataFrameHasColumnsCheck: custom_message",
                },
            ),
            ("name", StringType(), None,
                {
                    "got": StringType(),
                    "has_failed": False,
                    "message": "DataFrameHasColumnsCheck: Column 'name' exists in the DataFrame and it's of type: StringType()",
                },
            ),
            ("name", DoubleType(), None,
                {
                    "got": StringType(),
                    "has_failed": True,
                    "message": "DataFrameHasColumnsCheck: Column 'name' exists in the DataFrame but it's not of type: DoubleType()",
                },
            ),
            ("not_name", DoubleType(), None,
                {
                    "got": "name, age, height, is_student, birth_date, last_check_in",
                    "has_failed": True,
                    "message": "DataFrameHasColumnsCheck: Column 'not_name' doesn't exist in the DataFrame",
                },
            ),
            ("name", StringType(), "custom_message",
                {
                    "got": StringType(),
                    "has_failed": False,
                    "message": "DataFrameHasColumnsCheck: custom_message",
                },
            ),
            ("not_name", DoubleType(), "custom_message",
                {
                    "got": "name, age, height, is_student, birth_date, last_check_in",
                    "has_failed": True,
                    "message": "DataFrameHasColumnsCheck: custom_message",
                },
            ),
        ],
    )
    def test_eval_expectation(
        self,
        df_test,
        value,
        column,
        custom_message,
        expected_result,
    ):
        expectations = DataFrameHasColumnsCheck(column, value, custom_message)
        assert expectations.eval_expectation(df_test) == expected_result

    def test_eval_expectation_exception(self):
        expectations = DataFrameHasColumnsCheck("name", StringType(), "name")
        with pytest.raises(TypeError, match="DataFrameHasColumnsCheck: The target must be a Spark DataFrame, but got 'int'"):
            expectations.eval_expectation(1)
