import pytest
from src.sparkchecker.bin._column_expectations import (
    NonNullColumn,
    NullColumn,
    RlikeColumn,
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
        ]
    )
    data = [
        (
            "Alice",
            "AU",
            None,
            "swimming",
            25,
            1.60,
            True,
            r"[A-Z]{2}",
            r"[A-Z]{2}",
        ),
        ("Bob", "FR", None, None, 30, 1.75, False, r"[A-Z]{2}", r"[A-Z]{1}"),
        (
            "Charlie",
            "DE",
            None,
            "running",
            35,
            1.80,
            True,
            r"[A-Z]{2}",
            r"[A-Z]{3}",
        ),
    ]

    df = spark_session.createDataFrame(data, schema)
    return df


class TestNonNullColumn:

    @pytest.mark.parametrize(
        "column_name, value, message",
        [
            ("name", True, None),
            ("name", False, None),
            ("name", True, "hello world"),
        ],
    )
    def test_init(self, column_name, value, message):
        NonNullColumn(column_name, value, message)

    @pytest.mark.parametrize(
        "column_name, value, message, exception, match",
        [
            (
                "name",
                True,
                1,
                TypeError,
                r"Expected 'message' to be of type 'str', but got 'int'",
            ),
            (
                "name",
                1,
                None,
                TypeError,
                r"Argument `value` for NonNullColumn must be of type bool but got: ', <class 'int'>",
            ),
        ],
    )
    def test_init_exceptions(
        self, column_name, value, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            NonNullColumn(column_name, value, message)

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
            repr(NonNullColumn(column_name, value).constraint)
            == expected_constraint
        )

    @pytest.mark.parametrize(
        "column_name, value, custom_message, input_value, expected_message",
        [
            ("name", True, "custom message", True, "custom message"),
            ("name", True, "custom message", False, "custom message"),
            (
                "name",
                True,
                None,
                True,
                "The column name did not meet the expectation of NonNullColumn",
            ),
            (
                "name",
                True,
                None,
                False,
                "The column name did meet the expectation of NonNullColumn",
            ),
        ],
    )
    def test_get_message(
        self,
        column_name,
        value,
        custom_message,
        input_value,
        expected_message,
    ):
        expectations = NonNullColumn(column_name, value, custom_message)
        expectations.get_message(input_value)
        assert expectations.message == expected_message

    @pytest.mark.parametrize(
        "column_name, value, custom_message, expected_result",
        [
            (
                "name",
                True,
                None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "The column name did meet the expectation of NonNullColumn",
                },
            ),
            (
                "name",
                False,
                None,
                {
                    "example": {"name": "Alice"},
                    "got": 3,
                    "has_failed": True,
                    "message": "The column name did not meet the expectation of NullColumn",
                },
            ),
            (
                "name",
                True,
                "custom message",
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "custom message",
                },
            ),
            (
                "name",
                False,
                "custom message",
                {
                    "example": {"name": "Alice"},
                    "got": 3,
                    "has_failed": True,
                    "message": "custom message",
                },
            ),
            (
                "hobby",
                False,
                None,
                {
                    "example": {"hobby": "swimming"},
                    "got": 2,
                    "has_failed": True,
                    "message": "The column hobby did not meet the expectation of NullColumn",
                },
            ),
            (
                "hobby",
                True,
                None,
                {
                    "example": {"hobby": None},
                    "got": 1,
                    "has_failed": True,
                    "message": "The column hobby did not meet the expectation of NonNullColumn",
                },
            ),
            (
                "crypto",
                False,
                None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "The column crypto did meet the expectation of NullColumn",
                },
            ),
            (
                "crypto",
                True,
                None,
                {
                    "example": {"crypto": None},
                    "got": 3,
                    "has_failed": True,
                    "message": "The column crypto did not meet the expectation of NonNullColumn",
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
        expectations = NonNullColumn(column_name, value, custom_message)
        assert expectations.eval_expectation(df_test) == expected_result

    def test_eval_expectation_exception(self, df_test):
        expectations = NonNullColumn("name", True)
        with pytest.raises(
            ValueError, match="Column 'name' does not exist in the DataFrame"
        ):
            expectations.eval_expectation(df_test.drop("name"))


class TestNullColumn:

    @pytest.mark.parametrize(
        "column_name, value, message",
        [
            ("name", True, None),
            ("name", False, None),
            ("name", True, "hello world"),
        ],
    )
    def test_init(self, column_name, value, message):
        NullColumn(column_name, value, message)

    @pytest.mark.parametrize(
        "column_name, value, message, exception, match",
        [
            (
                "name",
                True,
                1,
                TypeError,
                r"Expected 'message' to be of type 'str', but got 'int'",
            ),
            (
                "name",
                1,
                None,
                TypeError,
                r"Argument `value` for NullColumn must be of type bool but got: ', <class 'int'>",
            ),
        ],
    )
    def test_init_exceptions(
        self, column_name, value, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            NullColumn(column_name, value, message)

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
            repr(NullColumn(column_name, value).constraint)
            == expected_constraint
        )

    @pytest.mark.parametrize(
        "column_name, value, custom_message, input_value, expected_message",
        [
            ("name", True, "custom message", True, "custom message"),
            ("name", True, "custom message", False, "custom message"),
            (
                "name",
                True,
                None,
                True,
                "The column name did not meet the expectation of NullColumn",
            ),
            (
                "name",
                True,
                None,
                False,
                "The column name did meet the expectation of NullColumn",
            ),
        ],
    )
    def test_get_message(
        self,
        column_name,
        value,
        custom_message,
        input_value,
        expected_message,
    ):
        expectations = NullColumn(column_name, value, custom_message)
        expectations.get_message(input_value)
        assert expectations.message == expected_message

    @pytest.mark.parametrize(
        "column_name, value, custom_message, expected_result",
        [
            (
                "name",
                True,
                None,
                {
                    "example": {"name": "Alice"},
                    "got": 3,
                    "has_failed": True,
                    "message": "The column name did not meet the expectation of NullColumn",
                },
            ),
            (
                "name",
                False,
                None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "The column name did meet the expectation of NonNullColumn",
                },
            ),
            (
                "name",
                False,
                "custom message",
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "custom message",
                },
            ),
            (
                "name",
                True,
                "custom message",
                {
                    "example": {"name": "Alice"},
                    "got": 3,
                    "has_failed": True,
                    "message": "custom message",
                },
            ),
            (
                "hobby",
                False,
                None,
                {
                    "example": {"hobby": None},
                    "got": 1,
                    "has_failed": True,
                    "message": "The column hobby did not meet the expectation of NonNullColumn",
                },
            ),
            (
                "hobby",
                True,
                None,
                {
                    "example": {"hobby": "swimming"},
                    "got": 2,
                    "has_failed": True,
                    "message": "The column hobby did not meet the expectation of NullColumn",
                },
            ),
            (
                "crypto",
                False,
                None,
                {
                    "example": {"crypto": None},
                    "got": 3,
                    "has_failed": True,
                    "message": "The column crypto did not meet the expectation of NonNullColumn",
                },
            ),
            (
                "crypto",
                True,
                None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "The column crypto did meet the expectation of NullColumn",
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
        expectations = NullColumn(column_name, value, custom_message)
        assert expectations.eval_expectation(df_test) == expected_result

    def test_eval_expectation_exception(self, df_test):
        expectations = NullColumn("name", True)
        with pytest.raises(
            ValueError, match="Column 'name' does not exist in the DataFrame"
        ):
            expectations.eval_expectation(df_test.drop("name"))


class TestRlikeColumn:

    @pytest.mark.parametrize(
        "column_name, value, message",
        [
            ("name", "regexp", None),
            ("name", "regexp", "hello world"),
        ],
    )
    def test_init(self, column_name, value, message):
        RlikeColumn(column_name, value, message)

    @pytest.mark.parametrize(
        "column_name, value, message, exception, match",
        [
            (
                "name",
                "regexp",
                1,
                TypeError,
                r"Expected 'message' to be of type 'str', but got 'int'",
            ),
            (
                "name",
                1,
                None,
                TypeError,
                r"Argument `value` for RlikeColumn must be of type str but got: ', <class 'int'>",
            ),
            (
                "name",
                None,
                None,
                TypeError,
                r"Argument `value` for RlikeColumn must be of type str but got: ', <class 'NoneType'>",
            ),
        ],
    )
    def test_init_exceptions(
        self, column_name, value, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            RlikeColumn(column_name, value, message)

    def test_constraint(self, spark_session):
        expectations = RlikeColumn("name", ".*")
        expectations.value = lit(r".*")
        assert repr(expectations.constraint) == "Column<'regexp(name, .*)'>"
        expectations = RlikeColumn("name", "alice")
        expectations.value = col("alice")
        assert repr(expectations.constraint) == "Column<'regexp(name, alice)'>"

    @pytest.mark.parametrize(
        "column_name, value, custom_message, input_value, expected_message",
        [
            ("name", r".*", "custom message", True, "custom message"),
            ("name", r".*", "custom message", False, "custom message"),
            (
                "name",
                r".*",
                None,
                True,
                "The column name did not respect the pattern `.*`",
            ),
            (
                "name",
                r".*",
                None,
                False,
                "The column name did respect the pattern `.*`",
            ),
        ],
    )
    def test_get_message(
        self,
        spark_session,
        column_name,
        value,
        custom_message,
        input_value,
        expected_message,
    ):
        expectations = RlikeColumn(column_name, value, custom_message)
        expectations.value = lit(value)
        expectations.get_message(input_value)
        assert expectations.message == expected_message

    @pytest.mark.parametrize(
        "column_name, value, custom_message, expected_result",
        [
            (
                "name",
                r".*",
                None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": r"The column name did respect the pattern `.*`",
                },
            ),
            (
                "name",
                r"/d",
                None,
                {
                    "example": {"name": "Alice"},
                    "got": 3,
                    "has_failed": True,
                    "message": r"The column name did not respect the pattern `/d`",
                },
            ),
            (
                "name",
                r".*",
                "custom message",
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": "custom message",
                },
            ),
            (
                "name",
                r"/d",
                "custom message",
                {
                    "example": {"name": "Alice"},
                    "got": 3,
                    "has_failed": True,
                    "message": "custom message",
                },
            ),
            (
                "country",
                "pattern_ok",
                None,
                {
                    "example": {},
                    "got": 0,
                    "has_failed": False,
                    "message": r"The column country did respect the pattern `pattern_ok`",
                },
            ),
            (
                "country",
                "pattern_nok",
                None,
                {
                    "example": {"country": "DE"},
                    "got": 1,
                    "has_failed": True,
                    "message": r"The column country did not respect the pattern `pattern_nok`",
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
        expectations = RlikeColumn(column_name, value, custom_message)
        assert expectations.eval_expectation(df_test) == expected_result

    def test_eval_expectation_exception(self, df_test):
        expectations = RlikeColumn("name", "crypto")
        with pytest.raises(
            ValueError, match="Column 'name' does not exist in the DataFrame"
        ):
            expectations.eval_expectation(df_test.drop("name"))
