from pyspark.sql import DataFrame

from .bin._expectations_factory import ExpectationsFactory
from .bin._logger import setup_logger
from .bin._utils import extract_base_path_and_filename, read_yaml_file
from .bin._yaml_parser import ExpectationsYamlParser


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

    stack = ExpectationsYamlParser(yaml_checks)

    stack.parse()

    compile_stack = ExpectationsFactory(self, stack.stacks)

    compile_stack.compile()

    name, path = extract_base_path_and_filename(path)

    setup_logger(name, path)


DataFrame.sparkChecker = sparkChecker
