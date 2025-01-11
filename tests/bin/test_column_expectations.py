import pytest
from src.sparkchecker.bin._column_expectations import (
    NonNullColumn,
    NullColumn,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    BooleanType,
)


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
        ]
    )
    data = [
        ("Alice", "AU", None, "swimming", 25, 1.60, True),
        ("Bob", "FR", None, None, 30, 1.75, False),
        ("Charlie", "DE", None, "running", 35, 1.80, True),
    ]

    df = spark_session.createDataFrame(data, schema)
    return df


class TestNonNullColumn:

    @pytest.mark.parametrize(
        "column_name, is_not_null, message",
        [
            ("name", True, None),
            ("name", False, None),
            ("name", True, "hello world"),
        ],
    )
    def test_init(self, column_name, is_not_null, message):
        NonNullColumn(column_name, is_not_null, message)

    @pytest.mark.parametrize(
        "column_name, is_not_null, message, exception, match",
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
        self, column_name, is_not_null, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            NonNullColumn(column_name, is_not_null, message)

    @pytest.mark.parametrize(
        "column_name, is_not_null, expected_constraint",
        [
            ("name", True, "Column<'(name IS NOT NULL)'>"),
            ("name", False, "Column<'(name IS NOT NULL)'>"),
        ],
    )
    def test_constraint(
        self, spark_session, column_name, is_not_null, expected_constraint
    ):
        assert (
            repr(NonNullColumn(column_name, is_not_null).constraint)
            == expected_constraint
        )

    @pytest.mark.parametrize(
        "column_name, is_not_null, custom_message, input_value, expected_message",
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
        is_not_null,
        custom_message,
        input_value,
        expected_message,
    ):
        expectations = NonNullColumn(column_name, is_not_null, custom_message)
        expectations.get_message(input_value)
        assert expectations.message == expected_message

    @pytest.mark.parametrize(
        "column_name, is_not_null, custom_message, expected_result",
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
        is_not_null,
        custom_message,
        expected_result,
    ):
        expectations = NonNullColumn(column_name, is_not_null, custom_message)
        assert expectations.eval_expectation(df_test) == expected_result

    def test_eval_expectation_exception(self, df_test):
        expectations = NonNullColumn("name", True)
        with pytest.raises(
            ValueError, match="Column 'name' does not exist in the DataFrame"
        ):
            expectations.eval_expectation(df_test.drop("name"))


class TestNullColumn:

    @pytest.mark.parametrize(
        "column_name, is_not_null, message",
        [
            ("name", True, None),
            ("name", False, None),
            ("name", True, "hello world"),
        ],
    )
    def test_init(self, column_name, is_not_null, message):
        NullColumn(column_name, is_not_null, message)

    @pytest.mark.parametrize(
        "column_name, is_not_null, message, exception, match",
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
        self, column_name, is_not_null, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            NullColumn(column_name, is_not_null, message)

    @pytest.mark.parametrize(
        "column_name, is_null, expected_constraint",
        [
            ("name", True, "Column<'(name IS NULL)'>"),
            ("name", False, "Column<'(name IS NULL)'>"),
        ],
    )
    def test_constraint(
        self, spark_session, column_name, is_null, expected_constraint
    ):
        assert (
            repr(NullColumn(column_name, is_null).constraint)
            == expected_constraint
        )

    @pytest.mark.parametrize(
        "column_name, is_not_null, custom_message, input_value, expected_message",
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
        is_not_null,
        custom_message,
        input_value,
        expected_message,
    ):
        expectations = NullColumn(column_name, is_not_null, custom_message)
        expectations.get_message(input_value)
        assert expectations.message == expected_message

    @pytest.mark.parametrize(
        "column_name, is_not_null, custom_message, expected_result",
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
        is_not_null,
        custom_message,
        expected_result,
    ):
        expectations = NullColumn(column_name, is_not_null, custom_message)
        assert expectations.eval_expectation(df_test) == expected_result

    def test_eval_expectation_exception(self, df_test):
        expectations = NullColumn("name", True)
        with pytest.raises(
            ValueError, match="Column 'name' does not exist in the DataFrame"
        ):
            expectations.eval_expectation(df_test.drop("name"))
