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

# Map operator strings to corresponding functions
OPERATOR_MAP = {
    "lower": lt,
    "lower_or_equal": le,
    "equal": eq,
    "different": ne,
    "higher": gt,
    "higher_or_equal": ge,
}

CONSTRAINT_CONSTRUCTOR = ["constraint", "strategy", "message"]
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
