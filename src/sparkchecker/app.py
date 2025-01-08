from pyspark.sql import DataFrame

from .bin._constraint_compiler import ConstraintCompiler
from .bin._logger import setup_logger
from .bin._utils import extract_base_path_and_filename, read_yaml_file
from .bin._yaml_parser import ConstraintYamlParser


def sparkChecker(
    self: DataFrame,
    path: str,
) -> None:
    """
    This function checks a DataFrame against a stack of checks.

    :param self: (DataFrame), the DataFrame to check

    :param path: (str), the path to the yaml file containing the checks

    :return: None
    """
    yaml_checks = read_yaml_file(path)

    stack = ConstraintYamlParser(yaml_checks)

    stack.run()

    compile_stack = ConstraintCompiler(self, stack.stacks)

    compile_stack.compile()

    name, path = extract_base_path_and_filename(path)

    setup_logger(name, path)


DataFrame.sparkChecker = sparkChecker
