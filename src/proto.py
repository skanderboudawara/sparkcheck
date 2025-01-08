"""
File used for prototyping: src/proto.py.

Will be removed in the final version.
"""

from sparkchecker import (
    ConstraintYamlParser,
    read_yaml_file,
)

yaml_checks = read_yaml_file(
    "/Users/skanderboudawara/Desktop/GitHub/sparkcheck/examples/check_df.yaml",
)
# from sparkchecker.bin._dataframe_utils import (
#     _IsEmpty,
#     _CountThreshold,
# )
# from sparkchecker.bin._constants import DATAFRAME_OPEARATIONS

from sparkchecker.bin._dataframe_utils import (
    _IsEmpty,
    _CountThreshold,
    _PartitionsCount,
)

from sparkchecker.bin._cols_utils import (
    _NonNullColumn,
    _NullColumn,
    _RlikeColumn,
    _IsInColumn,
    _ColumnCompare,
)

DF_OPERATIONS = {
    "count": _CountThreshold,
    "partitions": _PartitionsCount,
    "is_empty": _IsEmpty,

}
COL_OPERATION = {
    "not_null": _NonNullColumn,
    "is_null": _NullColumn,
    "pattern": _RlikeColumn,
    "in": _IsInColumn,
    "lower": _ColumnCompare,
    "lower_or_equal": _ColumnCompare,
    "equal": _ColumnCompare,
    "different": _ColumnCompare,
    "higher": _ColumnCompare,
    "higher_or_equal": _ColumnCompare,
}
class ConstraintCompiler:
    def __init__(self, stack, df):
        self.stack = stack
        self.df = df
        self.compiled_stack = []
    
    def compile(self):
        for check in self.stack:
            check_type = check["check"]
            if check_type in DF_OPERATIONS:
                check_instance = DF_OPERATIONS[check_type](df=self.df, **check)
                predicate = check_instance.predicate
                check.update({
                    "predicate": predicate[0],
                    "count_cases": predicate[1],
                })
                self.compiled_stack.append(check)
                continue
            if check_type == "column":
                check_instance = COL_OPERATION[check["operator"]]
                check_instance = check_instance(**check)
                predicate = self.df.filter(~check_instance.predicate).first()
                predicate = bool(not predicate)
                count_cases = 0
                if predicate:
                    count_cases = self.df.filter(check_instance.predicate).count()
                check.update({"predicate": predicate, "count_cases": count_cases})
                self.compiled_stack.append(check)
                continue

    @property
    def compiled(self):
        return self.compiled_stack


from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame(
    schema=["passenger_name", "passenger_age", "passenger_origins"],
    data=[
    ["John Doe", 30, "New York"],
        ["Jane Doe", 25, "Los Angeles"],
        ["Alice Smith", 28, "Chicago"],
        ["Bob Johnson", 35, "Houston"],
        ["Charlie Brown", 22, "Phoenix"],
    ],
)
stack = ConstraintYamlParser(yaml_checks)
stack.run()
stack.stacks
compile_stack = ConstraintCompiler(stack.stacks, df)
compile_stack.compile()
compile_stack.compiled
print(compile_stack.compiled)