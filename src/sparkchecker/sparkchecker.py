"""
This is the main file that contains the main function that is used to check the DataFrame
    against the checks in the yaml file.
"""

from pyspark.sql import DataFrame

from .bin._expectations_factory import ExpectationsFactory
from .bin._yaml_parser import (
    ExpectationsYamlParser,
    read_yaml_file,
    replace_keys_in_json,
)
from .constants import KEY_EQUIVALENT
from .ext._logger import setup_logger
from .ext._utils import extract_base_path_and_filename


def sparkChecker(  # noqa: N802
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

    yaml_checks = replace_keys_in_json(yaml_checks, KEY_EQUIVALENT)

    stack = ExpectationsYamlParser(yaml_checks)

    stack.parse()

    compile_stack = ExpectationsFactory(self, stack.stacks)

    compile_stack.compile()

    name, path = extract_base_path_and_filename(path)

    setup_logger(name, path)

    print(compile_stack.compiled)  # noqa: T201


DataFrame.sparkChecker = sparkChecker  # type: ignore
