"""
Core module for validating DataFrames against YAML-defined checks.

This is the main file that contains the main function that is
    used to check the DataFrame against the checks in the yaml file.
"""

from pyspark.sql import DataFrame

from ..bin._expectations_factory import ExpectationsFactory
from ..bin._report import ReportGenerator
from ..bin._yaml_parser import (
    ExpectationsYamlParser,
    read_yaml_file,
    replace_keys_in_json,
)
from ..constants import KEY_EQUIVALENT
from ..ext._logger import setup_logger
from ..ext._utils import split_base_file


def sparkChecker(  # noqa: N802
    self: DataFrame,
    path: str,
    *,
    raise_error: bool = True,
    print_log: bool = True,
    write_file: bool = True,
    file_path: str | None = None,
) -> None:  # pragma: no cover
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

    if file_path:
        name, path = split_base_file(file_path)

    else:
        name, path = split_base_file(path)

    logger = setup_logger(
        name,
        path,
        log_format=2,
        force_create=True,
        print_log=print_log,
        write_file=write_file,
    )

    ReportGenerator(logger, compile_stack.compiled, raise_error)


DataFrame.sparkChecker = sparkChecker  # type: ignore
