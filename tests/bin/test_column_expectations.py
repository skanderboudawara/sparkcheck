import pytest
import re
from abc import ABC, abstractmethod
from src.sparkchecker.bin._column_expectations import (
    ColNonNullCheck,
    ColNullCheck,
    ColRegexLikeCheck,
    ColIsInCheck,
    ColCompareCheck,
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


class TestColNonNullCheck(BaseClassColumnTest):

    @pytest.mark.parametrize(
        "column_name, value, message",
        [
            ("name", True, None),
            ("name", False, None),
            ("name", True, "hello world"),
        ],
    )
    def test_init(self, column_name, value, message):
        ColNonNullCheck(column_name, value, message)

    @pytest.mark.parametrize(
        "column_name, value, message, exception, match",
        [
            ("name", True, 1, TypeError, re.escape("ColNonNullCheck: the argument `message` does not correspond to the expected types '[str | NoneType]'. Got: int")),
            ("name", 1, None, TypeError, re.escape("ColNonNullCheck: the argument `value` does not correspond to the expected types '[bool]'. Got: int")),
            (None, True, None, TypeError, re.escape("ColNonNullCheck: the argument `column` does not correspond to the expected types '[str | Column]'. Got: NoneType")),
        ],
    )
    def test_init_exceptions(
        self, column_name, value, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            ColNonNullCheck(column_name, value, message)

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
            repr(ColNonNullCheck(column_name, value).constraint)
            == expected_constraint
        )

    @pytest.mark.parametrize(
        "column_name, value, custom_message, has_failed, expected_message",
        [
            ("name", True, "custom message", True, "ColNonNullCheck: custom message"),
            ("name", True, "custom message", False, "ColNonNullCheck: custom message"),
            ("name", True, None, True, "ColNonNullCheck: The column `name` did not meet the expectation of ColNonNullCheck"),
            ("name", True, None, False, "ColNonNullCheck: The column `name` did meet the expectation of ColNonNullCheck"),
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
        expectations = ColNonNullCheck(column_name, value, custom_message)
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
                    "message": "ColNonNullCheck: The column `name` did meet the expectation of ColNonNullCheck",
                },
            ),
            ("name", False, None,
                {
                    "example": {"name": "Alice"},
                    "got": 3,
                    "has_failed": True,
                    "message": "ColNullCheck: The column `name` did not meet the expectation of ColNullCheck",
                },
            ),
            ("name", True, "custom message",
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColNonNullCheck: custom message",
                },
            ),
            ("name", False, "custom message",
                {
                    "example": {"name": "Alice"},
                    "got": 3,
                    "has_failed": True,
                    "message": "ColNullCheck: custom message",
                },
            ),
            ("hobby", False, None,
                {
                    "example": {"hobby": "swimming"},
                    "got": 2,
                    "has_failed": True,
                    "message": "ColNullCheck: The column `hobby` did not meet the expectation of ColNullCheck",
                },
            ),
            ("hobby", True, None,
                {
                    "example": {"hobby": None},
                    "got": 1,
                    "has_failed": True,
                    "message": "ColNonNullCheck: The column `hobby` did not meet the expectation of ColNonNullCheck",
                },
            ),
            ("crypto", False, None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColNullCheck: The column `crypto` did meet the expectation of ColNullCheck",
                },
            ),
            ("crypto", True, None,
                {
                    "example": {"crypto": None},
                    "got": 3,
                    "has_failed": True,
                    "message": "ColNonNullCheck: The column `crypto` did not meet the expectation of ColNonNullCheck",
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
        expectations = ColNonNullCheck(column_name, value, custom_message)
        assert expectations.eval_expectation(df_test) == expected_result

    def test_eval_expectation_exception(self, df_test):
        expectations = ColNonNullCheck("name", True)
        with pytest.raises(ValueError, match="ColNonNullCheck: Column 'name' does not exist in the DataFrame"):
            expectations.eval_expectation(df_test.drop("name"))
        with pytest.raises(TypeError, match="ColNonNullCheck: The target must be a Spark DataFrame, but got 'int'"):
            expectations.eval_expectation(1)

    def test_eval_dataframe_empty(self, df_test_empty):
        expectations = ColNonNullCheck("name", True)
        expectation_result = expectations.eval_expectation(df_test_empty)
        assert expectation_result == {
            "got": "Empty DataFrame",
            "has_failed": False,
            "message": "ColNonNullCheck: The DataFrame is empty.",
        }



class TestColNullCheck(BaseClassColumnTest):

    @pytest.mark.parametrize(
        "column_name, value, message",
        [
            ("name", True, None),
            ("name", False, None),
            ("name", True, "hello world"),
        ],
    )
    def test_init(self, column_name, value, message):
        ColNullCheck(column_name, value, message)

    @pytest.mark.parametrize(
        "column_name, value, message, exception, match",
        [
            ("name", True, 1, TypeError, re.escape("ColNullCheck: the argument `message` does not correspond to the expected types '[str | NoneType]'. Got: int")),
            ("name", 1, None, TypeError, re.escape("ColNullCheck: the argument `value` does not correspond to the expected types '[bool]'. Got: int")),
            (None, True, None, TypeError, re.escape("ColNullCheck: the argument `column` does not correspond to the expected types '[str | Column]'. Got: NoneType")),
        ],
    )
    def test_init_exceptions(
        self, column_name, value, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            ColNullCheck(column_name, value, message)

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
            repr(ColNullCheck(column_name, value).constraint)
            == expected_constraint
        )

    @pytest.mark.parametrize(
        "column_name, value, custom_message, has_failed, expected_message",
        [
            ("name", True, "custom message", True, "ColNullCheck: custom message"),
            ("name", True, "custom message", False, "ColNullCheck: custom message"),
            ("name", True, None, True, "ColNullCheck: The column `name` did not meet the expectation of ColNullCheck"),
            ("name", True, None, False, "ColNullCheck: The column `name` did meet the expectation of ColNullCheck"),
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
        expectations = ColNullCheck(column_name, value, custom_message)
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
                    "message": "ColNullCheck: The column `name` did not meet the expectation of ColNullCheck",
                },
            ),
            ("name", False, None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColNonNullCheck: The column `name` did meet the expectation of ColNonNullCheck",
                },
            ),
            ("name", False, "custom message",
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColNonNullCheck: custom message",
                },
            ),
            ("name", True, "custom message",
                {
                    "example": {"name": "Alice"},
                    "got": 3,
                    "has_failed": True,
                    "message": "ColNullCheck: custom message",
                },
            ),
            ("hobby", False, None,
                {
                    "example": {"hobby": None},
                    "got": 1,
                    "has_failed": True,
                    "message": "ColNonNullCheck: The column `hobby` did not meet the expectation of ColNonNullCheck",
                },
            ),
            ("hobby", True, None,
                {
                    "example": {"hobby": "swimming"},
                    "got": 2,
                    "has_failed": True,
                    "message": "ColNullCheck: The column `hobby` did not meet the expectation of ColNullCheck",
                },
            ),
            ("crypto", False, None,
                {
                    "example": {"crypto": None},
                    "got": 3,
                    "has_failed": True,
                    "message": "ColNonNullCheck: The column `crypto` did not meet the expectation of ColNonNullCheck",
                },
            ),
            ("crypto", True, None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColNullCheck: The column `crypto` did meet the expectation of ColNullCheck",
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
        expectations = ColNullCheck(column_name, value, custom_message)
        assert expectations.eval_expectation(df_test) == expected_result

    def test_eval_expectation_exception(self, df_test):
        expectations = ColNullCheck("name", True)
        with pytest.raises(ValueError, match="ColNullCheck: Column 'name' does not exist in the DataFrame"):
            expectations.eval_expectation(df_test.drop("name"))
        with pytest.raises(TypeError, match="ColNullCheck: The target must be a Spark DataFrame, but got 'int'"):
            expectations.eval_expectation(1)

    def test_eval_dataframe_empty(self, df_test_empty):
        expectations = ColNullCheck("name", True)
        expectation_result = expectations.eval_expectation(df_test_empty)
        assert expectation_result == {
            "got": "Empty DataFrame",
            "has_failed": False,
            "message": "ColNullCheck: The DataFrame is empty.",
        }


class TestColRegexLikeCheck(BaseClassColumnTest):

    @pytest.mark.parametrize(
        "column_name, value, message",
        [
            ("name", "regexp", None),
            ("name", "regexp", "hello world"),
        ],
    )
    def test_init(self, column_name, value, message):
        ColRegexLikeCheck(column_name, value, message)

    @pytest.mark.parametrize(
        "column_name, value, message, exception, match",
        [
            ("name", "regexp", 1, TypeError, re.escape("ColRegexLikeCheck: the argument `message` does not correspond to the expected types '[str | NoneType]'. Got: int")),
            ("name", 1, None, TypeError, re.escape("ColRegexLikeCheck: the argument `value` does not correspond to the expected types '[str | Column]'. Got: int")),
            ("name", None, None, TypeError, re.escape("ColRegexLikeCheck: the argument `value` does not correspond to the expected types '[str | Column]'. Got: NoneType")),
            (None, True, None, TypeError, re.escape("ColRegexLikeCheck: the argument `column` does not correspond to the expected types '[str | Column]'. Got: NoneType")),
        ],
    )
    def test_init_exceptions(
        self, column_name, value, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            ColRegexLikeCheck(column_name, value, message)

    def test_constraint(self, spark_session):
        expectations = ColRegexLikeCheck("name", ".*")
        expectations.value = lit(r".*")
        assert repr(expectations.constraint) == "Column<'regexp(name, .*)'>"
        expectations = ColRegexLikeCheck("name", "alice")
        expectations.value = col("alice")
        assert repr(expectations.constraint) == "Column<'regexp(name, alice)'>"

    @pytest.mark.parametrize(
        "column_name, value, custom_message, has_failed, expected_message",
        [
            ("name", r".*", "custom message", True, "ColRegexLikeCheck: custom message"),
            ("name", r".*", "custom message", False, "ColRegexLikeCheck: custom message"),
            ("name", r".*", None, True, "ColRegexLikeCheck: The column `name` did not respect the pattern `.*`"),
            ("name", r".*", None, False, "ColRegexLikeCheck: The column `name` did respect the pattern `.*`"),
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
        expectations = ColRegexLikeCheck(column_name, value, custom_message)
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
                    "message": r"ColRegexLikeCheck: The column `name` did respect the pattern `.*`",
                },
            ),
            ("name", r"/d", None,
                {
                    "example": {"name": "Alice"},
                    "got": 3,
                    "has_failed": True,
                    "message": r"ColRegexLikeCheck: The column `name` did not respect the pattern `/d`",
                },
            ),
            ("name", r".*", "custom message",
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColRegexLikeCheck: custom message",
                },
            ),
            ("name",r"/d","custom message",
                {
                    "example": {"name": "Alice"},
                    "got": 3,
                    "has_failed": True,
                    "message": "ColRegexLikeCheck: custom message",
                },
            ),
            ("country", "pattern_ok", None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": r"ColRegexLikeCheck: The column `country` did respect the pattern `pattern_ok`",
                },
            ),
            ("country", "pattern_nok", None,
                {
                    "example": {"country": "DE"},
                    "got": 1,
                    "has_failed": True,
                    "message": r"ColRegexLikeCheck: The column `country` did not respect the pattern `pattern_nok`",
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
        expectations = ColRegexLikeCheck(column_name, value, custom_message)
        assert expectations.eval_expectation(df_test) == expected_result

    def test_eval_expectation_exception(self, df_test):
        expectations = ColRegexLikeCheck("name", "crypto")
        with pytest.raises(ValueError, match="ColRegexLikeCheck: Column 'name' does not exist in the DataFrame"):
            expectations.eval_expectation(df_test.drop("name"))
        with pytest.raises(TypeError, match="ColRegexLikeCheck: The target must be a Spark DataFrame, but got 'int'"):
            expectations.eval_expectation(1)

    def test_eval_dataframe_empty(self, df_test_empty):
        expectations = ColRegexLikeCheck("name", "crypto")
        expectation_result = expectations.eval_expectation(df_test_empty)
        assert expectation_result == {
            "got": "Empty DataFrame",
            "has_failed": False,
            "message": "ColRegexLikeCheck: The DataFrame is empty.",
        }


class TestColIsInCheck(BaseClassColumnTest):

    @pytest.mark.parametrize(
        "column_name, value, message",
        [
            ("country", ["AU", "FR"], None),
            ("country",  ["AU", "FR"], "hello world"),
        ],
    )
    def test_init(self, column_name, value, message):
        ColIsInCheck(column_name, value, message)

    @pytest.mark.parametrize(
        "column_name, value, message, exception, match",
        [
            ("country", ["AU"], 1, TypeError, re.escape("ColIsInCheck: the argument `message` does not correspond to the expected types '[str | NoneType]'. Got: int")),
            (None, None, None, TypeError, re.escape("ColIsInCheck: the argument `column` does not correspond to the expected types '[str | Column]'. Got: NoneType")),
        ],
    )
    def test_init_exceptions(
        self, column_name, value, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            ColIsInCheck(column_name, value, message)

    def test_constraint(self, spark_session):
        expectations = ColIsInCheck("country", ["AU"])
        assert repr(expectations.constraint) == "Column<'(country IN (AU))'>"
        expectations = ColIsInCheck("country", ["AU", None])
        assert repr(expectations.constraint) == "Column<'(country IN (AU, NULL))'>"
        expectations = ColIsInCheck("country", [col("name"), "AU"])
        assert repr(expectations.constraint) == "Column<'(country IN (name, AU))'>"
        expectations = ColIsInCheck("country", [col("name"), lit("AU")])
        assert repr(expectations.constraint) == "Column<'(country IN (name, AU))'>"

    @pytest.mark.parametrize(
        "column_name, value, custom_message, has_failed, expected_message",
        [
            ("country", ["AU"], "custom message", True, "ColIsInCheck: custom message"),
            ("country", ["AU"], "custom message", False, "ColIsInCheck: custom message"),
            ("country", ["AU"], None, True, "ColIsInCheck: The column `country` is not in `[AU]`"),
            ("country", ["AU"], None, False, "ColIsInCheck: The column `country` is in `[AU]`"),
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
        expectations = ColIsInCheck(column_name, value, custom_message)
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
                    "message": r"ColIsInCheck: The column `country` is in `[AU, DE, FR]`",
                },
            ),
            ("country", ["AU", "DE"], None,
                {
                    "example": {"country": "FR"},
                    "got": 1,
                    "has_failed": True,
                    "message": r"ColIsInCheck: The column `country` is not in `[AU, DE]`",
                },
            ),
            ("country", ["AU", "DE", "FR"], "custom message",
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColIsInCheck: custom message",
                },
            ),
            ("country", ["only_AU", "only_DE", "only_FR"], None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColIsInCheck: The column `country` is in `[only_AU, only_DE, only_FR]`",
                },
            ),
            ("country", ["only_AU", "only_DE", "only_FR", "only_TN"], None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColIsInCheck: The column `country` is in `[only_AU, only_DE, only_FR, only_TN]`",
                },
            ),
            ("country", ["only_AU", "only_DE", "FR"], None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColIsInCheck: The column `country` is in `[FR, only_AU, only_DE]`",
                },
            ),
            ("country", ["only_AU", "only_DE", "only_TN"], None,
                {
                    "example": {"country": "FR"},
                    "got": 1,
                    "has_failed": True,
                    "message": "ColIsInCheck: The column `country` is not in `[only_AU, only_DE, only_TN]`",
                },
            ),
            ("country", ["AU", "DE"], "custom message",
                {
                    "example": {"country": "FR"},
                    "got": 1,
                    "has_failed": True,
                    "message": "ColIsInCheck: custom message",
                },
            ),
            ("hobby", ["swimming", "running", None], None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColIsInCheck: The column `hobby` is in `[NoneObject, running, swimming]`",
                },
            ),
            ("hobby", ["swimming", "running"], None,
                {
                    "example": {"hobby": "NoneObject"},
                    "got": 1,
                    "has_failed": True,
                    "message": "ColIsInCheck: The column `hobby` is not in `[running, swimming]`",
                },
            ),
            ("is_student", [True], None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColIsInCheck: The column `is_student` is in `[True]`",
                },
            ),
            ("is_student", True, None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "ColIsInCheck: The column `is_student` is in `[True]`",
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
        expectations = ColIsInCheck(column_name, value, custom_message)
        assert expectations.eval_expectation(df_test) == expected_result

    def test_eval_expectation_exception(self, df_test):
        expectations = ColIsInCheck("country", "crypto")
        with pytest.raises(ValueError, match="ColIsInCheck: Column 'country' does not exist in the DataFrame"):
            expectations.eval_expectation(df_test.drop("country"))
        with pytest.raises(TypeError, match="ColIsInCheck: The target must be a Spark DataFrame, but got 'int'"):
            expectations.eval_expectation(1)

    def test_eval_dataframe_empty(self, df_test_empty):
        expectations = ColIsInCheck("name", True)
        expectation_result = expectations.eval_expectation(df_test_empty)
        assert expectation_result == {
            "got": "Empty DataFrame",
            "has_failed": False,
            "message": "ColIsInCheck: The DataFrame is empty.",
        }


class TestColCompareCheck(BaseClassColumnTest):

    @pytest.mark.parametrize(
        "column_name, value, operator, message",
        [
            ("age", 10, "higher", None),
            ("age",  10, "higher", "hello world"),
        ],
    )
    def test_init(self, column_name, value, operator, message):
        ColCompareCheck(column_name, value, operator, message)

    @pytest.mark.parametrize(
        "column_name, value, operator, message, exception, match",
        [
            ("age", 10, "lower", 1, TypeError, re.escape("ColCompareCheck: the argument `message` does not correspond to the expected types '[str | NoneType]'. Got: int")),
            ("age", 10, 1, None, TypeError, re.escape("ColCompareCheck: the argument `operator` does not correspond to the expected types '[str]'. Got: int")),
            ("age", 10, None, None, TypeError, re.escape("ColCompareCheck: the argument `operator` does not correspond to the expected types '[str]'. Got: NoneType")),
            ("age", ["A"], "lower", None, TypeError, re.escape("ColCompareCheck: the argument `value` does not correspond to the expected types '[str | float | int | Column | bool | NoneType]'. Got: list")),
            ("age", 10, "flower", None, ValueError, re.escape("ColCompareCheck: Invalid operator: 'flower'. Must be one of: '[lower, lower_or_equal, equal, different, higher, higher_or_equal]'")),
            (None, 10, "lower", None, TypeError, re.escape("ColCompareCheck: the argument `column` does not correspond to the expected types '[str | Column]'. Got: NoneType")),
        ],
    )
    def test_init_exceptions(
        self, column_name, value, operator, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            ColCompareCheck(column_name, value, operator, message)

    def test_constraint(self, spark_session):
        expectations = ColCompareCheck("age", 10, "lower")
        assert repr(expectations.constraint) == "Column<'(age < 10)'>"
        expectations = ColCompareCheck("age", 10, "higher")
        assert repr(expectations.constraint) == "Column<'(age > 10)'>"
        expectations = ColCompareCheck("age", 10, "lower_or_equal")
        assert repr(expectations.constraint) == "Column<'(age <= 10)'>"
        expectations = ColCompareCheck("age", 10, "higher_or_equal")
        assert repr(expectations.constraint) == "Column<'(age >= 10)'>"
        expectations = ColCompareCheck("age", "NoneObject", "equal")
        assert repr(expectations.constraint) == "Column<'(age = NoneObject)'>"
        expectations = ColCompareCheck("age", "NoneObject", "different")
        assert repr(expectations.constraint) == "Column<'(NOT (age = NoneObject))'>"


    @pytest.mark.parametrize(
        "column_name, value, operator, custom_message, has_failed, expected_message",
        [
            ("age", 10, "higher", "custom message", True, "ColCompareCheck: custom message"),
            ("age", 10, "higher", "custom message", False, "ColCompareCheck: custom message"),
            ("age", 10, "higher", None, True, "ColCompareCheck: The column `age` is not higher than `10`"),
            ("age", 10, "higher", None, False, "ColCompareCheck: The column `age` is higher than `10`"),
            ("age", 10, "equal", None, True, "ColCompareCheck: The column `age` is not equal to `10`"),
            ("age", 10, "equal", None, False, "ColCompareCheck: The column `age` is equal to `10`"),
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
        expectations = ColCompareCheck(column_name, value, operator, custom_message)
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
                    "message": r"ColCompareCheck: The column `age` is higher than `10`",
                },
            ),
            ("age", 10, "higher_or_equal", None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": r"ColCompareCheck: The column `age` is higher or equal than `10`",
                },
            ),
            ("age", 50, "lower_or_equal", None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": r"ColCompareCheck: The column `age` is lower or equal than `50`",
                },
            ),
            ("age", 10, "lower", None,
                {
                    "example": {"age": 25},
                    "got": 3,
                    "has_failed": True,
                    "message": r"ColCompareCheck: The column `age` is not lower than `10`",
                },
            ),
            ("is_student", True, "equal", None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": r"ColCompareCheck: The column `is_student` is equal to `True`",
                },
            ),
            ("only_NULL", None, "equal", None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": r"ColCompareCheck: The column `only_NULL` is equal to `None`",
                },
            ),
            ("is_student", True, "different", None,
                {
                    "example": {"is_student": True},
                    "got": 3,
                    "has_failed": True,
                    "message": r"ColCompareCheck: The column `is_student` is not different to `True`",
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
        expectations = ColCompareCheck(column_name, value, operator, custom_message)
        assert expectations.eval_expectation(df_test) == expected_result

    def test_eval_expectation_exception(self, df_test):
        expectations = ColCompareCheck("age", "10", "lower")
        with pytest.raises(ValueError, match="ColCompareCheck: Column 'age' does not exist in the DataFrame"):
            expectations.eval_expectation(df_test.drop("age"))
        with pytest.raises(TypeError, match="ColCompareCheck: The target must be a Spark DataFrame, but got 'int'"):
            expectations.eval_expectation(1)

    def test_eval_dataframe_empty(self, df_test_empty):
        expectations = ColCompareCheck("age", True, "equal")
        expectation_result = expectations.eval_expectation(df_test_empty)
        assert expectation_result == {
            "got": "Empty DataFrame",
            "has_failed": False,
            "message": "ColCompareCheck: The DataFrame is empty.",
        }
