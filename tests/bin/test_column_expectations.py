import pytest
from src.sparkchecker.bin._column_expectations import (
    NonNullColumn,
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
            StructField("hobby", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("height", DoubleType(), True),
            StructField("is_student", BooleanType(), True),
        ]
    )
    data = [
        ("Alice", "AU", "swimming", 25, 1.60, True),
        ("Bob", "FR", None, 30, 1.75, False),
        ("Charlie", "DE", "running", 35, 1.80, True),
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
                r"Argument is not_null `value` must be of type bool but got: ', <class 'int'>",
            ),
        ],
    )
    def test_init_exceptions(
        self, column_name, is_not_null, message, exception, match
    ):
        with pytest.raises(exception, match=match):
            NonNullColumn(column_name, is_not_null, message)

    def test_constraint(self, spark_session):
        assert (
            repr(NonNullColumn("name", True).constraint)
            == "Column<'(name IS NOT NULL)'>"
        )
        assert (
            repr(NonNullColumn("name", False).constraint)
            == "Column<'(name IS NOT NULL)'>"
        )

    @pytest.mark.parametrize(
        "column_name, is_not_null, custom_message, input_value, expected_message",
        [
            ("name", True, "custom message", True, "custom message"),
            ("name", True, "custom message", False, "custom message"),
            ("name", True, None, True, "The column name is Not Null"),
            ("name", True, None, False, "The column name is not Not Null"),
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
                    "message": "The column name is Not Null",
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
                    "message": "The column name is not Not Null",
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
