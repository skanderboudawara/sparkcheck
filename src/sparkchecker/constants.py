"""
This module contains constants used in the SparkChecker library.
"""

from operator import eq, ge, gt, le, lt, ne

from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

KEY_EQUIVALENT = {
    "from": "value",
    "than": "value",
    "to": "value",
    "is": "value",
    "values": "value",
    "equals": "equal",
    "lt": "lower",
    "gt": "higher",
    "greater": "higher",
    "lte": "lower_or_equal",
    "lower_or_equals": "lower_or_equal",
    "higher_or_equals": "higher_or_equal",
    "gte": "higher_or_equal",
    "messages": "message",
    "diff": "different",
    "difference": "different",
    "differ": "different",
    "non_null": "not_null",
    "like": "pattern",
    "rlike": "pattern",
    "isin": "in",
    "is_in": "in",
    "isnull": "is_null",
    "null": "is_null",
    "isempty": "is_empty",
    "empty": "is_empty",
    "exists": "exist",
    "has_column": "has_columns",
    "hascolumn": "has_columns",
    "columns": "has_columns",
}

# Map operator strings to corresponding functions
OPERATOR_MAP = {
    "lower": lt,
    "lower_or_equal": le,
    "equal": eq,
    "different": ne,
    "higher": gt,
    "higher_or_equal": ge,
}

COLUMN_TYPES = {
    "string": StringType(),
    "str": StringType(),
    "integer": IntegerType(),
    "int": IntegerType(),
    "long": LongType(),
    "float": FloatType(),
    "double": DoubleType(),
    "timestamp": TimestampType(),
    "date": DateType(),
    "boolean": BooleanType(),
    "bool": BooleanType(),
    "bytes": ByteType(),
    "decimal": DecimalType(),
}

CONSTRAINT_CONSTRUCTOR = [
    "value",
    "strategy",
    "message",
]

COLUMN_OPERATIONS = [
    "not_null",
    "is_null",
    "pattern",
    "in",
    "lower",
    "lower_or_equal",
    "equal",
    "different",
    "higher",
    "higher_or_equal",
]
