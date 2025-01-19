import re

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from sparkchecker.bin._expectations_factory import (
    ExpectationsFactory,
)


class TestExpectationsFactory:
    @pytest.fixture
    def sample_df(self, spark_session):
        return spark_session.createDataFrame([(1, "a"), (2, "b")], ["num", "letter"]).coalesce(1)

    @pytest.fixture
    def empty_df(self, spark_session):
        schema = StructType([
            StructField("num", IntegerType(), True),
            StructField("letter", StringType(), True),
        ])
        df = spark_session.createDataFrame([], schema)
        df = df.cache()
        return df

    def test_initialization(self, sample_df):
        stack = [{"check": "row_count"}]
        factory = ExpectationsFactory(sample_df, stack)
        assert isinstance(factory.df, DataFrame)
        assert factory.stack == stack
        assert factory.compiled_stack == []

    @pytest.mark.parametrize(("check", "expected"), [
        (
            {"value": 0, "strategy": "warn", "check": "count", "operator": "higher"},
            {
                "check": "count",
                "got": 2,
                "has_failed": False,
                "message": "DataFrameCountThresholdCheck: The DataFrame has 2 rows, which is higher than 0",
                "operator": "higher",
                "strategy": "warn",
                "value": 0,
            },
        ),
        (
            {"value": False, "strategy": "warn", "check": "is_empty"},
            {
                "check": "is_empty",
                "got": False,
                "has_failed": False,
                "message": "DataFrameIsEmptyCheck: The DataFrame is not empty",
                "strategy": "warn",
                "value": False,
            },
        ),
        (
            {"value": 0, "strategy": "warn", "check": "partitions", "operator": "higher"},
            {
                "check": "partitions",
                "got": 1,
                "has_failed": False,
                "message": "DataFramePartitionsCountCheck: The DataFrame has 1 partitions, which is higher than 0",
                "operator": "higher",
                "strategy": "warn",
                "value": 0,
            },
        ),
        (
            {"column": "letter", "value": StringType(), "check": "has_columns"},
            {
                "check": "has_columns",
                "column": "letter",
                "got": StringType(),
                "has_failed": False,
                "message": "DataFrameHasColumnsCheck: Column 'letter' exists in the DataFrame and it's of type: StringType()",
                "value": StringType(),
            },
        ),
    ])
    def test_compile_dataframe_operation(self, sample_df, check, expected):
        result = ExpectationsFactory._compile_dataframe_operation(sample_df, check)
        assert result == expected

    def test_compile_with_empty_stack(self, sample_df):
        factory = ExpectationsFactory(sample_df, [])
        with pytest.raises(ValueError, match=re.escape("No checks provided.")):
            factory.compile()

    def test_compile_with_invalid_check_type(self, sample_df):
        factory = ExpectationsFactory(sample_df, [{"check": "invalid_check"}])
        with pytest.raises(ValueError, match="Unknown check type: invalid_check"):
            factory.compile()

    def test_compile_with_missing_check_type(self, sample_df):
        factory = ExpectationsFactory(sample_df, [{}])
        with pytest.raises(ValueError, match="Check type is missing"):
            factory.compile()

    def test_compile_with_empty_dataframe(self, empty_df):
        check = [{"value": 15, "strategy": "fail", "column": "num", "check": "column", "operator": "lower"}]
        factory = ExpectationsFactory(empty_df, check)
        factory.compile()
        assert factory.compiled == [
            {
                "check": "column",
                "has_failed": True,
                "message": "DataFrame is empty. No column checks can be performed.",
            },
        ]

    def test_full_compilation(self, sample_df):
        check = [{"value": 15, "strategy": "fail", "column": "num", "check": "column", "operator": "lower"}]
        factory = ExpectationsFactory(sample_df, check)
        factory.compile()
        assert factory.compiled == [
            {
                "check": "column",
                "has_failed": False,
                "strategy": "fail",
                "value": 15,
                "got": 0,
                "operator": "lower",
                "message": "ColCompareCheck: The column `num` is lower than `15`",
                "column": "num",
                "example": {},
            },
        ]

    def test_compiled_property(self, sample_df):
        factory = ExpectationsFactory(sample_df, [])
        assert factory.compiled == []
