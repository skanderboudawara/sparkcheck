import pytest
import re
from abc import ABC, abstractmethod
from src.sparkchecker.bin._column_expectations import (
    ColumnNonNullExpectation,
    ColumnNullExpectation,
    ColumnRegexLikeExpectation,
    ColumnIsInExpectation,
    ColumnCompareExpectation,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    BooleanType,
)
from pyspark.sql.functions import col, lit


@pytest.fixture
def df_test(spark_session):
    schema = StructType(
        [
            StructField("name"       , StringType() , True),
            StructField("country"    , StringType() , True),
            StructField("crypto"     , StringType() , True),
            StructField("hobby"      , StringType() , True),
            StructField("age"        , IntegerType(), True),
            StructField("height"     , DoubleType() , True),
            StructField("is_student" , BooleanType(), True),
            StructField("pattern_ok" , StringType() , True),
            StructField("pattern_nok", StringType() , True),
            StructField("only_AU"    , StringType() , True),
            StructField("only_FR"    , StringType() , True),
            StructField("only_DE"    , StringType() , True),
            StructField("only_TN"    , StringType() , True),
            StructField("only_NULL"  , StringType() , True),
        ]
    )
    data = [
    #   (name     , country, crypto, hobby     , age, height, is_student, pattern_ok , pattern_nok, only_AU, only_FR, only_DE, only_TN, only_NULL),  # noqa
        ("Alice"  , "AU"   , None  , "swimming", 25 , 1.60  , True      , r"[A-Z]{2}", r"[A-Z]{2}", "AU"   , "FR"   , "DE"   , "TN"   , None     ),
        ("Bob"    , "FR"   , None  , None      , 30 , 1.75  , True      , r"[A-Z]{2}", r"[A-Z]{1}", "AU"   , "FR"   , "DE"   , "TN"   , None     ),
        ("Charlie", "DE"   , None  , "running" , 35 , 1.80  , True      , r"[A-Z]{2}", r"[A-Z]{3}", "AU"   , "FR"   , "DE"   , "TN"   , None     ),
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

class BaseClassColumnTest(ABC):

    @abstractmethod
    def test_init(self):
        pass

    @abstractmethod
    def test_init_exceptions(self):
        pass

    @abstractmethod
    def test_constraint(self):
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

    @abstractmethod
    def test_eval_dataframe_empty(self):
        pass


class TestColumnNonNullExpectation(BaseClassColumnTest):

    @pytest.mark.parametrize(
        "column_name, value, message",
        [
            ("name", True, None),
            ("name", False, None),
            ("name", True, "hello world"),
        ],
    )
    def test_init(self, column_name, value, message):
        ColumnNonNullExpectation(column_name, value, message)

    @pytest.mark.parametrize(
        "column_name, value, message, exception, match",
        [
            ("name", True, 1, TypeError, re.escape("ColumnNonNullExpectation: the argument `message` does not correspond to the expected types '[str | NoneType]'. Got: int")),
            ("name", 1, None, TypeError, re.escape("ColumnNonNullExpectation: the argument `value` does not correspond to the expected types '[bool]'. Got: int")),
            (None, True, None, TypeError, re.escape("ColumnNonNullExpectation: the argument `column` does not correspond to the expected types '[str | Column]'. Got: NoneType")),
        ],
    )
    def test_init_exceptions(
        self, column_name, value, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            ColumnNonNullExpectation(column_name, value, message)

    @pytest.mark.parametrize(
        "column_name, value, expected_constraint",
        [
            ("name", True, "Column<'(name IS NOT NULL)'>"),
            ("name", False, "Column<'(name IS NOT NULL)'>"),
        ],
    )
    def test_constraint(
        self, spark_session, column_name, value, expected_constraint
    ):
        assert (
            repr(ColumnNonNullExpectation(column_name, value).constraint)
            == expected_constraint
        )

    @pytest.mark.parametrize(
        "column_name, value, custom_message, has_failed, expected_message",
        [
            ("name", True, "custom message", True, "ColumnNonNullExpectation: custom message"),
            ("name", True, "custom message", False, "ColumnNonNullExpectation: custom message"),
            ("name", True, None, True, "ColumnNonNullExpectation: The column `name` did not meet the expectation of ColumnNonNullExpectation"),
            ("name", True, None, False, "ColumnNonNullExpectation: The column `name` did meet the expectation of ColumnNonNullExpectation"),
        ],
    )
    def test_get_message(
        self,
        column_name,
        value,
        custom_message,
        has_failed,
        expected_message,
    ):
        expectations = ColumnNonNullExpectation(column_name, value, custom_message)
        expectations.get_message(has_failed)
        assert expectations.message == expected_message

    @pytest.mark.parametrize(
        "column_name, value, custom_message, expected_result",
        [
            ("name", True, None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColumnNonNullExpectation: The column `name` did meet the expectation of ColumnNonNullExpectation",
                },
            ),
            ("name", False, None,
                {
                    "example": {"name": "Alice"},
                    "got": 3,
                    "has_failed": True,
                    "message": "ColumnNullExpectation: The column `name` did not meet the expectation of ColumnNullExpectation",
                },
            ),
            ("name", True, "custom message",
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColumnNonNullExpectation: custom message",
                },
            ),
            ("name", False, "custom message",
                {
                    "example": {"name": "Alice"},
                    "got": 3,
                    "has_failed": True,
                    "message": "ColumnNullExpectation: custom message",
                },
            ),
            ("hobby", False, None,
                {
                    "example": {"hobby": "swimming"},
                    "got": 2,
                    "has_failed": True,
                    "message": "ColumnNullExpectation: The column `hobby` did not meet the expectation of ColumnNullExpectation",
                },
            ),
            ("hobby", True, None,
                {
                    "example": {"hobby": None},
                    "got": 1,
                    "has_failed": True,
                    "message": "ColumnNonNullExpectation: The column `hobby` did not meet the expectation of ColumnNonNullExpectation",
                },
            ),
            ("crypto", False, None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColumnNullExpectation: The column `crypto` did meet the expectation of ColumnNullExpectation",
                },
            ),
            ("crypto", True, None,
                {
                    "example": {"crypto": None},
                    "got": 3,
                    "has_failed": True,
                    "message": "ColumnNonNullExpectation: The column `crypto` did not meet the expectation of ColumnNonNullExpectation",
                },
            ),
        ],
    )
    def test_eval_expectation(
        self,
        df_test,
        column_name,
        value,
        custom_message,
        expected_result,
    ):
        expectations = ColumnNonNullExpectation(column_name, value, custom_message)
        assert expectations.eval_expectation(df_test) == expected_result

    def test_eval_expectation_exception(self, df_test):
        expectations = ColumnNonNullExpectation("name", True)
        with pytest.raises(ValueError, match="ColumnNonNullExpectation: Column 'name' does not exist in the DataFrame"):
            expectations.eval_expectation(df_test.drop("name"))
        with pytest.raises(TypeError, match="ColumnNonNullExpectation: The target must be a Spark DataFrame, but got 'int'"):
            expectations.eval_expectation(1)

    def test_eval_dataframe_empty(self, df_test_empty):
        expectations = ColumnNonNullExpectation("name", True)
        expectation_result = expectations.eval_expectation(df_test_empty)
        assert expectation_result == {
            "got": "Empty DataFrame",
            "has_failed": False,
            "message": "ColumnNonNullExpectation: The DataFrame is empty.",
        }



class TestColumnNullExpectation(BaseClassColumnTest):

    @pytest.mark.parametrize(
        "column_name, value, message",
        [
            ("name", True, None),
            ("name", False, None),
            ("name", True, "hello world"),
        ],
    )
    def test_init(self, column_name, value, message):
        ColumnNullExpectation(column_name, value, message)

    @pytest.mark.parametrize(
        "column_name, value, message, exception, match",
        [
            ("name", True, 1, TypeError, re.escape("ColumnNullExpectation: the argument `message` does not correspond to the expected types '[str | NoneType]'. Got: int")),
            ("name", 1, None, TypeError, re.escape("ColumnNullExpectation: the argument `value` does not correspond to the expected types '[bool]'. Got: int")),
            (None, True, None, TypeError, re.escape("ColumnNullExpectation: the argument `column` does not correspond to the expected types '[str | Column]'. Got: NoneType")),
        ],
    )
    def test_init_exceptions(
        self, column_name, value, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            ColumnNullExpectation(column_name, value, message)

    @pytest.mark.parametrize(
        "column_name, value, expected_constraint",
        [
            ("name", True, "Column<'(name IS NULL)'>"),
            ("name", False, "Column<'(name IS NULL)'>"),
        ],
    )
    def test_constraint(
        self, spark_session, column_name, value, expected_constraint
    ):
        assert (
            repr(ColumnNullExpectation(column_name, value).constraint)
            == expected_constraint
        )

    @pytest.mark.parametrize(
        "column_name, value, custom_message, has_failed, expected_message",
        [
            ("name", True, "custom message", True, "ColumnNullExpectation: custom message"),
            ("name", True, "custom message", False, "ColumnNullExpectation: custom message"),
            ("name", True, None, True, "ColumnNullExpectation: The column `name` did not meet the expectation of ColumnNullExpectation"),
            ("name", True, None, False, "ColumnNullExpectation: The column `name` did meet the expectation of ColumnNullExpectation"),
        ],
    )
    def test_get_message(
        self,
        column_name,
        value,
        custom_message,
        has_failed,
        expected_message,
    ):
        expectations = ColumnNullExpectation(column_name, value, custom_message)
        expectations.get_message(has_failed)
        assert expectations.message == expected_message

    @pytest.mark.parametrize(
        "column_name, value, custom_message, expected_result",
        [
            ("name", True, None,
                {
                    "example": {"name": "Alice"},
                    "got": 3,
                    "has_failed": True,
                    "message": "ColumnNullExpectation: The column `name` did not meet the expectation of ColumnNullExpectation",
                },
            ),
            ("name", False, None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColumnNonNullExpectation: The column `name` did meet the expectation of ColumnNonNullExpectation",
                },
            ),
            ("name", False, "custom message",
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColumnNonNullExpectation: custom message",
                },
            ),
            ("name", True, "custom message",
                {
                    "example": {"name": "Alice"},
                    "got": 3,
                    "has_failed": True,
                    "message": "ColumnNullExpectation: custom message",
                },
            ),
            ("hobby", False, None,
                {
                    "example": {"hobby": None},
                    "got": 1,
                    "has_failed": True,
                    "message": "ColumnNonNullExpectation: The column `hobby` did not meet the expectation of ColumnNonNullExpectation",
                },
            ),
            ("hobby", True, None,
                {
                    "example": {"hobby": "swimming"},
                    "got": 2,
                    "has_failed": True,
                    "message": "ColumnNullExpectation: The column `hobby` did not meet the expectation of ColumnNullExpectation",
                },
            ),
            ("crypto", False, None,
                {
                    "example": {"crypto": None},
                    "got": 3,
                    "has_failed": True,
                    "message": "ColumnNonNullExpectation: The column `crypto` did not meet the expectation of ColumnNonNullExpectation",
                },
            ),
            ("crypto", True, None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColumnNullExpectation: The column `crypto` did meet the expectation of ColumnNullExpectation",
                },
            ),
        ],
    )
    def test_eval_expectation(
        self,
        df_test,
        column_name,
        value,
        custom_message,
        expected_result,
    ):
        expectations = ColumnNullExpectation(column_name, value, custom_message)
        assert expectations.eval_expectation(df_test) == expected_result

    def test_eval_expectation_exception(self, df_test):
        expectations = ColumnNullExpectation("name", True)
        with pytest.raises(ValueError, match="ColumnNullExpectation: Column 'name' does not exist in the DataFrame"):
            expectations.eval_expectation(df_test.drop("name"))
        with pytest.raises(TypeError, match="ColumnNullExpectation: The target must be a Spark DataFrame, but got 'int'"):
            expectations.eval_expectation(1)

    def test_eval_dataframe_empty(self, df_test_empty):
        expectations = ColumnNullExpectation("name", True)
        expectation_result = expectations.eval_expectation(df_test_empty)
        assert expectation_result == {
            "got": "Empty DataFrame",
            "has_failed": False,
            "message": "ColumnNullExpectation: The DataFrame is empty.",
        }


class TestColumnRegexLikeExpectation(BaseClassColumnTest):

    @pytest.mark.parametrize(
        "column_name, value, message",
        [
            ("name", "regexp", None),
            ("name", "regexp", "hello world"),
        ],
    )
    def test_init(self, column_name, value, message):
        ColumnRegexLikeExpectation(column_name, value, message)

    @pytest.mark.parametrize(
        "column_name, value, message, exception, match",
        [
            ("name", "regexp", 1, TypeError, re.escape("ColumnRegexLikeExpectation: the argument `message` does not correspond to the expected types '[str | NoneType]'. Got: int")),
            ("name", 1, None, TypeError, re.escape("ColumnRegexLikeExpectation: the argument `value` does not correspond to the expected types '[str | Column]'. Got: int")),
            ("name", None, None, TypeError, re.escape("ColumnRegexLikeExpectation: the argument `value` does not correspond to the expected types '[str | Column]'. Got: NoneType")),
            (None, True, None, TypeError, re.escape("ColumnRegexLikeExpectation: the argument `column` does not correspond to the expected types '[str | Column]'. Got: NoneType")),
        ],
    )
    def test_init_exceptions(
        self, column_name, value, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            ColumnRegexLikeExpectation(column_name, value, message)

    def test_constraint(self, spark_session):
        expectations = ColumnRegexLikeExpectation("name", ".*")
        expectations.value = lit(r".*")
        assert repr(expectations.constraint) == "Column<'regexp(name, .*)'>"
        expectations = ColumnRegexLikeExpectation("name", "alice")
        expectations.value = col("alice")
        assert repr(expectations.constraint) == "Column<'regexp(name, alice)'>"

    @pytest.mark.parametrize(
        "column_name, value, custom_message, has_failed, expected_message",
        [
            ("name", r".*", "custom message", True, "ColumnRegexLikeExpectation: custom message"),
            ("name", r".*", "custom message", False, "ColumnRegexLikeExpectation: custom message"),
            ("name", r".*", None, True, "ColumnRegexLikeExpectation: The column `name` did not respect the pattern `.*`"),
            ("name", r".*", None, False, "ColumnRegexLikeExpectation: The column `name` did respect the pattern `.*`"),
        ],
    )
    def test_get_message(
        self,
        spark_session,
        column_name,
        value,
        custom_message,
        has_failed,
        expected_message,
    ):
        expectations = ColumnRegexLikeExpectation(column_name, value, custom_message)
        expectations.value = lit(value)
        expectations.get_message(has_failed)
        assert expectations.message == expected_message

    @pytest.mark.parametrize(
        "column_name, value, custom_message, expected_result",
        [
            ("name", r".*", None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": r"ColumnRegexLikeExpectation: The column `name` did respect the pattern `.*`",
                },
            ),
            ("name", r"/d", None,
                {
                    "example": {"name": "Alice"},
                    "got": 3,
                    "has_failed": True,
                    "message": r"ColumnRegexLikeExpectation: The column `name` did not respect the pattern `/d`",
                },
            ),
            ("name", r".*", "custom message",
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColumnRegexLikeExpectation: custom message",
                },
            ),
            ("name",r"/d","custom message",
                {
                    "example": {"name": "Alice"},
                    "got": 3,
                    "has_failed": True,
                    "message": "ColumnRegexLikeExpectation: custom message",
                },
            ),
            ("country", "pattern_ok", None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": r"ColumnRegexLikeExpectation: The column `country` did respect the pattern `pattern_ok`",
                },
            ),
            ("country", "pattern_nok", None,
                {
                    "example": {"country": "DE"},
                    "got": 1,
                    "has_failed": True,
                    "message": r"ColumnRegexLikeExpectation: The column `country` did not respect the pattern `pattern_nok`",
                },
            ),
        ],
    )
    def test_eval_expectation(
        self,
        df_test,
        column_name,
        value,
        custom_message,
        expected_result,
    ):
        expectations = ColumnRegexLikeExpectation(column_name, value, custom_message)
        assert expectations.eval_expectation(df_test) == expected_result

    def test_eval_expectation_exception(self, df_test):
        expectations = ColumnRegexLikeExpectation("name", "crypto")
        with pytest.raises(ValueError, match="ColumnRegexLikeExpectation: Column 'name' does not exist in the DataFrame"):
            expectations.eval_expectation(df_test.drop("name"))
        with pytest.raises(TypeError, match="ColumnRegexLikeExpectation: The target must be a Spark DataFrame, but got 'int'"):
            expectations.eval_expectation(1)

    def test_eval_dataframe_empty(self, df_test_empty):
        expectations = ColumnRegexLikeExpectation("name", "crypto")
        expectation_result = expectations.eval_expectation(df_test_empty)
        assert expectation_result == {
            "got": "Empty DataFrame",
            "has_failed": False,
            "message": "ColumnRegexLikeExpectation: The DataFrame is empty.",
        }


class TestColumnIsInExpectation(BaseClassColumnTest):

    @pytest.mark.parametrize(
        "column_name, value, message",
        [
            ("country", ["AU", "FR"], None),
            ("country",  ["AU", "FR"], "hello world"),
        ],
    )
    def test_init(self, column_name, value, message):
        ColumnIsInExpectation(column_name, value, message)

    @pytest.mark.parametrize(
        "column_name, value, message, exception, match",
        [
            ("country", ["AU"], 1, TypeError, re.escape("ColumnIsInExpectation: the argument `message` does not correspond to the expected types '[str | NoneType]'. Got: int")),
            (None, None, None, TypeError, re.escape("ColumnIsInExpectation: the argument `column` does not correspond to the expected types '[str | Column]'. Got: NoneType")),
        ],
    )
    def test_init_exceptions(
        self, column_name, value, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            ColumnIsInExpectation(column_name, value, message)

    def test_constraint(self, spark_session):
        expectations = ColumnIsInExpectation("country", ["AU"])
        assert repr(expectations.constraint) == "Column<'(country IN (AU))'>"
        expectations = ColumnIsInExpectation("country", ["AU", None])
        assert repr(expectations.constraint) == "Column<'(country IN (AU, NULL))'>"
        expectations = ColumnIsInExpectation("country", [col("name"), "AU"])
        assert repr(expectations.constraint) == "Column<'(country IN (name, AU))'>"
        expectations = ColumnIsInExpectation("country", [col("name"), lit("AU")])
        assert repr(expectations.constraint) == "Column<'(country IN (name, AU))'>"

    @pytest.mark.parametrize(
        "column_name, value, custom_message, has_failed, expected_message",
        [
            ("country", ["AU"], "custom message", True, "ColumnIsInExpectation: custom message"),
            ("country", ["AU"], "custom message", False, "ColumnIsInExpectation: custom message"),
            ("country", ["AU"], None, True, "ColumnIsInExpectation: The column `country` is not in `[AU]`"),
            ("country", ["AU"], None, False, "ColumnIsInExpectation: The column `country` is in `[AU]`"),
        ],
    )
    def test_get_message(
        self,
        spark_session,
        column_name,
        value,
        custom_message,
        has_failed,
        expected_message,
    ):
        expectations = ColumnIsInExpectation(column_name, value, custom_message)
        expectations.expected = "AU"
        expectations.get_message(has_failed)
        assert expectations.message == expected_message

    @pytest.mark.parametrize(
        "column_name, value, custom_message, expected_result",
        [
            ("country", ["AU", "DE", "FR"], None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": r"ColumnIsInExpectation: The column `country` is in `[AU, DE, FR]`",
                },
            ),
            ("country", ["AU", "DE"], None,
                {
                    "example": {"country": "FR"},
                    "got": 1,
                    "has_failed": True,
                    "message": r"ColumnIsInExpectation: The column `country` is not in `[AU, DE]`",
                },
            ),
            ("country", ["AU", "DE", "FR"], "custom message",
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColumnIsInExpectation: custom message",
                },
            ),
            ("country", ["only_AU", "only_DE", "only_FR"], None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColumnIsInExpectation: The column `country` is in `[only_AU, only_DE, only_FR]`",
                },
            ),
            ("country", ["only_AU", "only_DE", "only_FR", "only_TN"], None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColumnIsInExpectation: The column `country` is in `[only_AU, only_DE, only_FR, only_TN]`",
                },
            ),
            ("country", ["only_AU", "only_DE", "FR"], None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColumnIsInExpectation: The column `country` is in `[FR, only_AU, only_DE]`",
                },
            ),
            ("country", ["only_AU", "only_DE", "only_TN"], None,
                {
                    "example": {"country": "FR"},
                    "got": 1,
                    "has_failed": True,
                    "message": "ColumnIsInExpectation: The column `country` is not in `[only_AU, only_DE, only_TN]`",
                },
            ),
            ("country", ["AU", "DE"], "custom message",
                {
                    "example": {"country": "FR"},
                    "got": 1,
                    "has_failed": True,
                    "message": "ColumnIsInExpectation: custom message",
                },
            ),
            ("hobby", ["swimming", "running", None], None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColumnIsInExpectation: The column `hobby` is in `[NoneObject, running, swimming]`",
                },
            ),
            ("hobby", ["swimming", "running"], None,
                {
                    "example": {"hobby": "NoneObject"},
                    "got": 1,
                    "has_failed": True,
                    "message": "ColumnIsInExpectation: The column `hobby` is not in `[running, swimming]`",
                },
            ),
            ("is_student", [True], None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColumnIsInExpectation: The column `is_student` is in `[True]`",
                },
            ),
            ("is_student", True, None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColumnIsInExpectation: The column `is_student` is in `[True]`",
                },
            ),
        ],
    )
    def test_eval_expectation(
        self,
        df_test,
        column_name,
        value,
        custom_message,
        expected_result,
    ):
        expectations = ColumnIsInExpectation(column_name, value, custom_message)
        assert expectations.eval_expectation(df_test) == expected_result

    def test_eval_expectation_exception(self, df_test):
        expectations = ColumnIsInExpectation("country", "crypto")
        with pytest.raises(ValueError, match="ColumnIsInExpectation: Column 'country' does not exist in the DataFrame"):
            expectations.eval_expectation(df_test.drop("country"))
        with pytest.raises(TypeError, match="ColumnIsInExpectation: The target must be a Spark DataFrame, but got 'int'"):
            expectations.eval_expectation(1)

    def test_eval_dataframe_empty(self, df_test_empty):
        expectations = ColumnIsInExpectation("name", True)
        expectation_result = expectations.eval_expectation(df_test_empty)
        assert expectation_result == {
            "got": "Empty DataFrame",
            "has_failed": False,
            "message": "ColumnIsInExpectation: The DataFrame is empty.",
        }


class TestColumnCompareExpectation(BaseClassColumnTest):

    @pytest.mark.parametrize(
        "column_name, value, operator, message",
        [
            ("age", 10, "higher", None),
            ("age",  10, "higher", "hello world"),
        ],
    )
    def test_init(self, column_name, value, operator, message):
        ColumnCompareExpectation(column_name, value, operator, message)

    @pytest.mark.parametrize(
        "column_name, value, operator, message, exception, match",
        [
            ("age", 10, "lower", 1, TypeError, re.escape("ColumnCompareExpectation: the argument `message` does not correspond to the expected types '[str | NoneType]'. Got: int")),
            ("age", 10, 1, None, TypeError, re.escape("ColumnCompareExpectation: the argument `operator` does not correspond to the expected types '[str]'. Got: int")),
            ("age", 10, None, None, TypeError, re.escape("ColumnCompareExpectation: the argument `operator` does not correspond to the expected types '[str]'. Got: NoneType")),
            ("age", ["A"], "lower", None, TypeError, re.escape("ColumnCompareExpectation: the argument `value` does not correspond to the expected types '[str | float | int | Column | bool | NoneType]'. Got: list")),
            ("age", 10, "flower", None, ValueError, re.escape("ColumnCompareExpectation: Invalid operator: 'flower'. Must be one of: '[lower, lower_or_equal, equal, different, higher, higher_or_equal]'")),
            (None, 10, "lower", None, TypeError, re.escape("ColumnCompareExpectation: the argument `column` does not correspond to the expected types '[str | Column]'. Got: NoneType")),
        ],
    )
    def test_init_exceptions(
        self, column_name, value, operator, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            ColumnCompareExpectation(column_name, value, operator, message)

    def test_constraint(self, spark_session):
        expectations = ColumnCompareExpectation("age", 10, "lower")
        assert repr(expectations.constraint) == "Column<'(age < 10)'>"
        expectations = ColumnCompareExpectation("age", 10, "higher")
        assert repr(expectations.constraint) == "Column<'(age > 10)'>"
        expectations = ColumnCompareExpectation("age", 10, "lower_or_equal")
        assert repr(expectations.constraint) == "Column<'(age <= 10)'>"
        expectations = ColumnCompareExpectation("age", 10, "higher_or_equal")
        assert repr(expectations.constraint) == "Column<'(age >= 10)'>"
        expectations = ColumnCompareExpectation("age", "NoneObject", "equal")
        assert repr(expectations.constraint) == "Column<'(age = NoneObject)'>"
        expectations = ColumnCompareExpectation("age", "NoneObject", "different")
        assert repr(expectations.constraint) == "Column<'(NOT (age = NoneObject))'>"


    @pytest.mark.parametrize(
        "column_name, value, operator, custom_message, has_failed, expected_message",
        [
            ("age", 10, "higher", "custom message", True, "ColumnCompareExpectation: custom message"),
            ("age", 10, "higher", "custom message", False, "ColumnCompareExpectation: custom message"),
            ("age", 10, "higher", None, True, "ColumnCompareExpectation: The column `age` is not higher than `10`"),
            ("age", 10, "higher", None, False, "ColumnCompareExpectation: The column `age` is higher than `10`"),
            ("age", 10, "equal", None, True, "ColumnCompareExpectation: The column `age` is not equal to `10`"),
            ("age", 10, "equal", None, False, "ColumnCompareExpectation: The column `age` is equal to `10`"),
        ],
    )
    def test_get_message(
        self,
        spark_session,
        column_name,
        value,
        operator,
        custom_message,
        has_failed,
        expected_message,
    ):
        expectations = ColumnCompareExpectation(column_name, value, operator, custom_message)
        expectations.expected = value
        expectations.get_message(has_failed)
        assert expectations.message == expected_message

    @pytest.mark.parametrize(
        "column_name, value, operator, custom_message, expected_result",
        [
            ("age", 10, "higher", None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": r"ColumnCompareExpectation: The column `age` is higher than `10`",
                },
            ),
            ("age", 10, "higher_or_equal", None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": r"ColumnCompareExpectation: The column `age` is higher or equal than `10`",
                },
            ),
            ("age", 50, "lower_or_equal", None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": r"ColumnCompareExpectation: The column `age` is lower or equal than `50`",
                },
            ),
            ("age", 10, "lower", None,
                {
                    "example": {"age": 25},
                    "got": 3,
                    "has_failed": True,
                    "message": r"ColumnCompareExpectation: The column `age` is not lower than `10`",
                },
            ),
            ("is_student", True, "equal", None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": r"ColumnCompareExpectation: The column `is_student` is equal to `True`",
                },
            ),
            ("only_NULL", None, "equal", None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": r"ColumnCompareExpectation: The column `only_NULL` is equal to `None`",
                },
            ),
            ("is_student", True, "different", None,
                {
                    "example": {"is_student": True},
                    "got": 3,
                    "has_failed": True,
                    "message": r"ColumnCompareExpectation: The column `is_student` is not different to `True`",
                },
            ),
        ],
    )
    def test_eval_expectation(
        self,
        df_test,
        column_name,
        value,
        operator,
        custom_message,
        expected_result,
    ):
        expectations = ColumnCompareExpectation(column_name, value, operator, custom_message)
        assert expectations.eval_expectation(df_test) == expected_result

    def test_eval_expectation_exception(self, df_test):
        expectations = ColumnCompareExpectation("age", "10", "lower")
        with pytest.raises(ValueError, match="ColumnCompareExpectation: Column 'age' does not exist in the DataFrame"):
            expectations.eval_expectation(df_test.drop("age"))
        with pytest.raises(TypeError, match="ColumnCompareExpectation: The target must be a Spark DataFrame, but got 'int'"):
            expectations.eval_expectation(1)

    def test_eval_dataframe_empty(self, df_test_empty):
        expectations = ColumnCompareExpectation("age", True, "equal")
        expectation_result = expectations.eval_expectation(df_test_empty)
        assert expectation_result == {
            "got": "Empty DataFrame",
            "has_failed": False,
            "message": "ColumnCompareExpectation: The DataFrame is empty.",
        }
