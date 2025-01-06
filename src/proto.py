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


stack = ConstraintYamlParser(yaml_checks)
stack.run()
a = stack.stacks
