import pytest
import re
from abc import ABC, abstractmethod
from src.sparkchecker.bin._column_expectations import (
    NonNullColumnExpectation,
    NullColumnExpectation,
    RegexLikeColumnExpectation,
    IsInColumnExpectation,
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
            StructField("name", StringType(), True),
            StructField("country", StringType(), True),
            StructField("crypto", StringType(), True),
            StructField("hobby", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("height", DoubleType(), True),
            StructField("is_student", BooleanType(), True),
            StructField("pattern_ok", StringType(), True),
            StructField("pattern_nok", StringType(), True),
            StructField("only_AU", StringType(), True),
            StructField("only_FR", StringType(), True),
            StructField("only_DE", StringType(), True),
            StructField("only_TN", StringType(), True),
        ]
    )
    data = [
        ("Alice","AU",None,"swimming",25,1.60,True,r"[A-Z]{2}",r"[A-Z]{2}", "AU", "FR", "DE", "TN"),
        ("Bob", "FR", None, None, 30, 1.75, True, r"[A-Z]{2}", r"[A-Z]{1}", "AU", "FR", "DE", "TN"),
        ("Charlie","DE",None,"running",35,1.80,True,r"[A-Z]{2}",r"[A-Z]{3}", "AU", "FR", "DE", "TN"),
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


class TestNonNullColumnExpectation(BaseClassColumnTest):

    @pytest.mark.parametrize(
        "column_name, value, message",
        [
            ("name", True, None),
            ("name", False, None),
            ("name", True, "hello world"),
        ],
    )
    def test_init(self, column_name, value, message):
        NonNullColumnExpectation(column_name, value, message)

    @pytest.mark.parametrize(
        "column_name, value, message, exception, match",
        [
            ("name", True, 1, TypeError, re.escape("NonNullColumnExpectation: the argument `message` does not correspond to the expected types '[str | NoneType]'. Got: int")),
            ("name", 1, None, TypeError, re.escape("NonNullColumnExpectation: the argument `value` does not correspond to the expected types '[bool]'. Got: int")),
            (None, True, None, TypeError, re.escape("NonNullColumnExpectation: the argument `column` does not correspond to the expected types '[str | Column]'. Got: NoneType")),
        ],
    )
    def test_init_exceptions(
        self, column_name, value, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            NonNullColumnExpectation(column_name, value, message)

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
            repr(NonNullColumnExpectation(column_name, value).constraint)
            == expected_constraint
        )

    @pytest.mark.parametrize(
        "column_name, value, custom_message, has_failed, expected_message",
        [
            ("name", True, "custom message", True, "NonNullColumnExpectation: custom message"),
            ("name", True, "custom message", False, "NonNullColumnExpectation: custom message"),
            ("name", True, None, True, "NonNullColumnExpectation: The column `name` did not meet the expectation of NonNullColumnExpectation"),
            ("name", True, None, False, "NonNullColumnExpectation: The column `name` did meet the expectation of NonNullColumnExpectation"),
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
        expectations = NonNullColumnExpectation(column_name, value, custom_message)
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
                    "message": "NonNullColumnExpectation: The column `name` did meet the expectation of NonNullColumnExpectation",
                },
            ),
            ("name", False, None,
                {
                    "example": {"name": "Alice"},
                    "got": 3,
                    "has_failed": True,
                    "message": "NullColumnExpectation: The column `name` did not meet the expectation of NullColumnExpectation",
                },
            ),
            ("name", True, "custom message",
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "NonNullColumnExpectation: custom message",
                },
            ),
            ("name", False, "custom message",
                {
                    "example": {"name": "Alice"},
                    "got": 3,
                    "has_failed": True,
                    "message": "NullColumnExpectation: custom message",
                },
            ),
            ("hobby", False, None,
                {
                    "example": {"hobby": "swimming"},
                    "got": 2,
                    "has_failed": True,
                    "message": "NullColumnExpectation: The column `hobby` did not meet the expectation of NullColumnExpectation",
                },
            ),
            ("hobby", True, None,
                {
                    "example": {"hobby": None},
                    "got": 1,
                    "has_failed": True,
                    "message": "NonNullColumnExpectation: The column `hobby` did not meet the expectation of NonNullColumnExpectation",
                },
            ),
            ("crypto", False, None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "NullColumnExpectation: The column `crypto` did meet the expectation of NullColumnExpectation",
                },
            ),
            ("crypto", True, None,
                {
                    "example": {"crypto": None},
                    "got": 3,
                    "has_failed": True,
                    "message": "NonNullColumnExpectation: The column `crypto` did not meet the expectation of NonNullColumnExpectation",
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
        expectations = NonNullColumnExpectation(column_name, value, custom_message)
        assert expectations.eval_expectation(df_test) == expected_result

    def test_eval_expectation_exception(self, df_test):
        expectations = NonNullColumnExpectation("name", True)
        with pytest.raises(ValueError, match="NonNullColumnExpectation: Column 'name' does not exist in the DataFrame"):
            expectations.eval_expectation(df_test.drop("name"))
        with pytest.raises(TypeError, match="NonNullColumnExpectation: The target must be a Spark DataFrame, but got 'int'"):
            expectations.eval_expectation(1)

    def test_eval_dataframe_empty(self, df_test_empty):
        expectations = NonNullColumnExpectation("name", True)
        expectation_result = expectations.eval_expectation(df_test_empty)
        assert expectation_result == {
            "got": "Empty DataFrame",
            "has_failed": False,
            "message": "NonNullColumnExpectation: The DataFrame is empty.",
        }



class TestNullColumnExpectation(BaseClassColumnTest):

    @pytest.mark.parametrize(
        "column_name, value, message",
        [
            ("name", True, None),
            ("name", False, None),
            ("name", True, "hello world"),
        ],
    )
    def test_init(self, column_name, value, message):
        NullColumnExpectation(column_name, value, message)

    @pytest.mark.parametrize(
        "column_name, value, message, exception, match",
        [
            ("name", True, 1, TypeError, re.escape("NullColumnExpectation: the argument `message` does not correspond to the expected types '[str | NoneType]'. Got: int")),
            ("name", 1, None, TypeError, re.escape("NullColumnExpectation: the argument `value` does not correspond to the expected types '[bool]'. Got: int")),
            (None, True, None, TypeError, re.escape("NullColumnExpectation: the argument `column` does not correspond to the expected types '[str | Column]'. Got: NoneType")),
        ],
    )
    def test_init_exceptions(
        self, column_name, value, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            NullColumnExpectation(column_name, value, message)

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
            repr(NullColumnExpectation(column_name, value).constraint)
            == expected_constraint
        )

    @pytest.mark.parametrize(
        "column_name, value, custom_message, has_failed, expected_message",
        [
            ("name", True, "custom message", True, "NullColumnExpectation: custom message"),
            ("name", True, "custom message", False, "NullColumnExpectation: custom message"),
            ("name", True, None, True, "NullColumnExpectation: The column `name` did not meet the expectation of NullColumnExpectation"),
            ("name", True, None, False, "NullColumnExpectation: The column `name` did meet the expectation of NullColumnExpectation"),
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
        expectations = NullColumnExpectation(column_name, value, custom_message)
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
                    "message": "NullColumnExpectation: The column `name` did not meet the expectation of NullColumnExpectation",
                },
            ),
            ("name", False, None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "NonNullColumnExpectation: The column `name` did meet the expectation of NonNullColumnExpectation",
                },
            ),
            ("name", False, "custom message",
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "NonNullColumnExpectation: custom message",
                },
            ),
            ("name", True, "custom message",
                {
                    "example": {"name": "Alice"},
                    "got": 3,
                    "has_failed": True,
                    "message": "NullColumnExpectation: custom message",
                },
            ),
            ("hobby", False, None,
                {
                    "example": {"hobby": None},
                    "got": 1,
                    "has_failed": True,
                    "message": "NonNullColumnExpectation: The column `hobby` did not meet the expectation of NonNullColumnExpectation",
                },
            ),
            ("hobby", True, None,
                {
                    "example": {"hobby": "swimming"},
                    "got": 2,
                    "has_failed": True,
                    "message": "NullColumnExpectation: The column `hobby` did not meet the expectation of NullColumnExpectation",
                },
            ),
            ("crypto", False, None,
                {
                    "example": {"crypto": None},
                    "got": 3,
                    "has_failed": True,
                    "message": "NonNullColumnExpectation: The column `crypto` did not meet the expectation of NonNullColumnExpectation",
                },
            ),
            ("crypto", True, None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "NullColumnExpectation: The column `crypto` did meet the expectation of NullColumnExpectation",
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
        expectations = NullColumnExpectation(column_name, value, custom_message)
        assert expectations.eval_expectation(df_test) == expected_result

    def test_eval_expectation_exception(self, df_test):
        expectations = NullColumnExpectation("name", True)
        with pytest.raises(ValueError, match="NullColumnExpectation: Column 'name' does not exist in the DataFrame"):
            expectations.eval_expectation(df_test.drop("name"))
        with pytest.raises(TypeError, match="NullColumnExpectation: The target must be a Spark DataFrame, but got 'int'"):
            expectations.eval_expectation(1)

    def test_eval_dataframe_empty(self, df_test_empty):
        expectations = NullColumnExpectation("name", True)
        expectation_result = expectations.eval_expectation(df_test_empty)
        assert expectation_result == {
            "got": "Empty DataFrame",
            "has_failed": False,
            "message": "NullColumnExpectation: The DataFrame is empty.",
        }


class TestRegexLikeColumnExpectation(BaseClassColumnTest):

    @pytest.mark.parametrize(
        "column_name, value, message",
        [
            ("name", "regexp", None),
            ("name", "regexp", "hello world"),
        ],
    )
    def test_init(self, column_name, value, message):
        RegexLikeColumnExpectation(column_name, value, message)

    @pytest.mark.parametrize(
        "column_name, value, message, exception, match",
        [
            ("name", "regexp", 1, TypeError, re.escape("RegexLikeColumnExpectation: the argument `message` does not correspond to the expected types '[str | NoneType]'. Got: int")),
            ("name", 1, None, TypeError, re.escape("RegexLikeColumnExpectation: the argument `value` does not correspond to the expected types '[str | Column]'. Got: int")),
            ("name", None, None, TypeError, re.escape("RegexLikeColumnExpectation: the argument `value` does not correspond to the expected types '[str | Column]'. Got: NoneType")),
            (None, True, None, TypeError, re.escape("RegexLikeColumnExpectation: the argument `column` does not correspond to the expected types '[str | Column]'. Got: NoneType")),
        ],
    )
    def test_init_exceptions(
        self, column_name, value, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            RegexLikeColumnExpectation(column_name, value, message)

    def test_constraint(self, spark_session):
        expectations = RegexLikeColumnExpectation("name", ".*")
        expectations.value = lit(r".*")
        assert repr(expectations.constraint) == "Column<'regexp(name, .*)'>"
        expectations = RegexLikeColumnExpectation("name", "alice")
        expectations.value = col("alice")
        assert repr(expectations.constraint) == "Column<'regexp(name, alice)'>"

    @pytest.mark.parametrize(
        "column_name, value, custom_message, has_failed, expected_message",
        [
            ("name", r".*", "custom message", True, "RegexLikeColumnExpectation: custom message"),
            ("name", r".*", "custom message", False, "RegexLikeColumnExpectation: custom message"),
            ("name", r".*", None, True, "RegexLikeColumnExpectation: The column `name` did not respect the pattern `.*`"),
            ("name", r".*", None, False, "RegexLikeColumnExpectation: The column `name` did respect the pattern `.*`"),
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
        expectations = RegexLikeColumnExpectation(column_name, value, custom_message)
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
                    "message": r"RegexLikeColumnExpectation: The column `name` did respect the pattern `.*`",
                },
            ),
            ("name", r"/d", None,
                {
                    "example": {"name": "Alice"},
                    "got": 3,
                    "has_failed": True,
                    "message": r"RegexLikeColumnExpectation: The column `name` did not respect the pattern `/d`",
                },
            ),
            ("name", r".*", "custom message",
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "RegexLikeColumnExpectation: custom message",
                },
            ),
            ("name",r"/d","custom message",
                {
                    "example": {"name": "Alice"},
                    "got": 3,
                    "has_failed": True,
                    "message": "RegexLikeColumnExpectation: custom message",
                },
            ),
            ("country", "pattern_ok", None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": r"RegexLikeColumnExpectation: The column `country` did respect the pattern `pattern_ok`",
                },
            ),
            ("country", "pattern_nok", None,
                {
                    "example": {"country": "DE"},
                    "got": 1,
                    "has_failed": True,
                    "message": r"RegexLikeColumnExpectation: The column `country` did not respect the pattern `pattern_nok`",
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
        expectations = RegexLikeColumnExpectation(column_name, value, custom_message)
        assert expectations.eval_expectation(df_test) == expected_result

    def test_eval_expectation_exception(self, df_test):
        expectations = RegexLikeColumnExpectation("name", "crypto")
        with pytest.raises(ValueError, match="RegexLikeColumnExpectation: Column 'name' does not exist in the DataFrame"):
            expectations.eval_expectation(df_test.drop("name"))
        with pytest.raises(TypeError, match="RegexLikeColumnExpectation: The target must be a Spark DataFrame, but got 'int'"):
            expectations.eval_expectation(1)

    def test_eval_dataframe_empty(self, df_test_empty):
        expectations = RegexLikeColumnExpectation("name", "crypto")
        expectation_result = expectations.eval_expectation(df_test_empty)
        assert expectation_result == {
            "got": "Empty DataFrame",
            "has_failed": False,
            "message": "RegexLikeColumnExpectation: The DataFrame is empty.",
        }


class TestIsInColumnExpectation(BaseClassColumnTest):

    @pytest.mark.parametrize(
        "column_name, value, message",
        [
            ("country", ["AU", "FR"], None),
            ("country",  ["AU", "FR"], "hello world"),
        ],
    )
    def test_init(self, column_name, value, message):
        IsInColumnExpectation(column_name, value, message)

    @pytest.mark.parametrize(
        "column_name, value, message, exception, match",
        [
            ("country", ["AU"], 1, TypeError, re.escape("IsInColumnExpectation: the argument `message` does not correspond to the expected types '[str | NoneType]'. Got: int")),
            (None, None, None, TypeError, re.escape("IsInColumnExpectation: the argument `column` does not correspond to the expected types '[str | Column]'. Got: NoneType")),
        ],
    )
    def test_init_exceptions(
        self, column_name, value, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            IsInColumnExpectation(column_name, value, message)

    def test_constraint(self, spark_session):
        expectations = IsInColumnExpectation("country", ["AU"])
        assert repr(expectations.constraint) == "Column<'(country IN (AU))'>"
        expectations = IsInColumnExpectation("country", ["AU", None])
        assert repr(expectations.constraint) == "Column<'(country IN (AU, NULL))'>"
        expectations = IsInColumnExpectation("country", [col("name"), "AU"])
        assert repr(expectations.constraint) == "Column<'(country IN (name, AU))'>"
        expectations = IsInColumnExpectation("country", [col("name"), lit("AU")])
        assert repr(expectations.constraint) == "Column<'(country IN (name, AU))'>"

    @pytest.mark.parametrize(
        "column_name, value, custom_message, has_failed, expected_message",
        [
            ("country", ["AU"], "custom message", True, "IsInColumnExpectation: custom message"),
            ("country", ["AU"], "custom message", False, "IsInColumnExpectation: custom message"),
            ("country", ["AU"], None, True, "IsInColumnExpectation: The column `country` is not in `[AU]`"),
            ("country", ["AU"], None, False, "IsInColumnExpectation: The column `country` is in `[AU]`"),
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
        expectations = IsInColumnExpectation(column_name, value, custom_message)
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
                    "message": r"IsInColumnExpectation: The column `country` is in `[AU, DE, FR]`",
                },
            ),
            ("country", ["AU", "DE"], None,
                {
                    "example": {"country": "FR"},
                    "got": 1,
                    "has_failed": True,
                    "message": r"IsInColumnExpectation: The column `country` is not in `[AU, DE]`",
                },
            ),
            ("country", ["AU", "DE", "FR"], "custom message",
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "IsInColumnExpectation: custom message",
                },
            ),
            ("country", ["only_AU", "only_DE", "only_FR"], None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "IsInColumnExpectation: The column `country` is in `[only_AU, only_DE, only_FR]`",
                },
            ),
            ("country", ["only_AU", "only_DE", "only_FR", "only_TN"], None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "IsInColumnExpectation: The column `country` is in `[only_AU, only_DE, only_FR, only_TN]`",
                },
            ),
            ("country", ["only_AU", "only_DE", "FR"], None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "IsInColumnExpectation: The column `country` is in `[FR, only_AU, only_DE]`",
                },
            ),
            ("country", ["only_AU", "only_DE", "only_TN"], None,
                {
                    "example": {"country": "FR"},
                    "got": 1,
                    "has_failed": True,
                    "message": "IsInColumnExpectation: The column `country` is not in `[only_AU, only_DE, only_TN]`",
                },
            ),
            ("country", ["AU", "DE"], "custom message",
                {
                    "example": {"country": "FR"},
                    "got": 1,
                    "has_failed": True,
                    "message": "IsInColumnExpectation: custom message",
                },
            ),
            ("hobby", ["swimming", "running", None], None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "IsInColumnExpectation: The column `hobby` is in `[NULL, running, swimming]`",
                },
            ),
            ("hobby", ["swimming", "running"], None,
                {
                    "example": {"hobby": "NULL"},
                    "got": 1,
                    "has_failed": True,
                    "message": "IsInColumnExpectation: The column `hobby` is not in `[running, swimming]`",
                },
            ),
            ("is_student", [True], None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "IsInColumnExpectation: The column `is_student` is in `[True]`",
                },
            ),
            ("is_student", True, None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "IsInColumnExpectation: The column `is_student` is in `[True]`",
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
        expectations = IsInColumnExpectation(column_name, value, custom_message)
        assert expectations.eval_expectation(df_test) == expected_result

    def test_eval_expectation_exception(self, df_test):
        expectations = IsInColumnExpectation("country", "crypto")
        with pytest.raises(ValueError, match="IsInColumnExpectation: Column 'country' does not exist in the DataFrame"):
            expectations.eval_expectation(df_test.drop("country"))
        with pytest.raises(TypeError, match="IsInColumnExpectation: The target must be a Spark DataFrame, but got 'int'"):
            expectations.eval_expectation(1)

    def test_eval_dataframe_empty(self, df_test_empty):
        expectations = IsInColumnExpectation("name", True)
        expectation_result = expectations.eval_expectation(df_test_empty)
        assert expectation_result == {
            "got": "Empty DataFrame",
            "has_failed": False,
            "message": "IsInColumnExpectation: The DataFrame is empty.",
        }
