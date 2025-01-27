"""
This module is responsible for generating a report based on the predicates.
"""

import logging

from ..ext._exceptions import SparkCheckerError


class ReportGenerator:
    """
    This class generates a report based on the predicates that have been.
    """

    INFO = 20
    WARN = 30
    ERROR = 40

    def __init__(
        self,
        logger: logging.Logger,
        predicates: list,
        raise_error: bool,
    ) -> None:
        """
        This function initializes the ReportGenerator class.

        :param logger: (logging.Logger) The logger to use.
        :param predicates: (list) The list of predicates to generate a report.
        :param raise_error: (bool) Whether to raise an error if any of
            the predicates have failed.
        :return: None
        """
        self.logger = logger
        self.predicates = predicates
        self._raise_error = raise_error
        self.raises: list[str] = []
        self._run()

    @staticmethod
    def _create_table(example: dict) -> str:
        """
        Creates a simple ASCII table from a single key-value pair in the input.

        :param example: (dict) The key-value pair to create a table for.
        :return: (str) The ASCII table.
        """
        col_name, col_value = next(iter(example.items()))
        col_value = str(col_value)
        max_width = max(len(col_name), len(col_value)) + 4

        def format_row(content: str) -> str:
            return f"| {content}{' ' * (max_width - len(content) - 2)}|"

        def format_border(max_width: int, sep: str) -> str:
            return "+" + sep * max_width + "+"

        # Construct the table
        table = [
            format_border(max_width, "="),
            format_row(col_name),
            format_border(max_width, "-"),
            format_row(col_value),
            format_border(max_width, "="),
        ]

        return "\n".join(table)

    def _create_fail(self, predicate: dict) -> tuple[int, str]:
        """
        This function creates a failure message for a predicate.

        :param predicate: (dict) The predicate that has failed.
        :return: (tuple) The log level and the message.
        """
        start = (
            "⚠️ WARN"
            if predicate.get("strategy", "fail") == "warn"
            else "❌ FAILED"
        )
        log_level = (
            self.WARN
            if predicate.get("strategy", "fail") == "warn"
            else self.ERROR
        )

        # Construct the message
        msg_parts = [
            f"{start} - Check {predicate['check']} has failed",
            predicate["message"],
            f"Number of rows failed: {predicate['got']}",
        ]

        # Add example table if applicable
        if isinstance(predicate["example"], dict):
            msg_parts.append("Example:")
            msg_parts.append(self._create_table(predicate["example"]))

        msg = "\n".join(msg_parts)

        return log_level, msg

    def _create_success(self, predicate: dict) -> tuple[int, str]:
        """
        This function creates a success message for a predicate.

        :param predicate: (dict) The predicate that has succeeded.
        :return: (tuple) The log level and the message.
        """
        msg = f"✅ PASSED - Check {predicate['check']} has succeeded"
        return self.INFO, msg

    def append_raises(self, msg: str, strategy: str) -> None:
        """
        This function appends a message to the raises list based on strategy.

        :param msg: (str) The message to append.
        :param strategy: (str) The strategy to use.
        :return: None
        """
        if strategy == "fail":
            self.raises.append(msg)

    def send_to_log(self, level: int, msg: str) -> None:
        """
        This function sends a message to the logger.

        @TODO: in future releases it will be an api for HTML

        :param level: (int) The log level to use.
        :param msg: (str) The message to send.
        :return: None
        """
        self.logger.log(level, msg)

    def raise_error(self) -> None:
        """
        This function raises an error if any of the predicates have failed.

        :return: None
        :raises: (SparkCheckerError) If any of the predicates have failed.
        """
        if self.raises and self._raise_error:
            concat_msgs = "\n\n".join(self.raises)
            concat_msgs += f"\n\n Total Failures: {len(self.raises)}"
            raise SparkCheckerError(
                SparkCheckerError.PREDICATE_FAILED,
                concat_msgs,
            )

    def _log_summary(self) -> None:
        """
        This function logs a summary of the report.

        :param: None
        :return: None
        """
        counts = {
            "total": len(self.predicates),
            "warns": sum(
                1
                for predicate in self.predicates
                if (
                    (predicate.get("strategy", "fail") == "warn")
                    and predicate["has_failed"]
                )
            ),
            "fails": sum(
                1
                for predicate in self.predicates
                if (
                    (predicate.get("strategy", "fail") == "fail")
                    and predicate["has_failed"]
                )
            ),
            "passes": sum(
                1
                for predicate in self.predicates
                if not predicate["has_failed"]
            ),
        }

        messages = [
            f"Total Number of Checks: {counts['total']}",
            f"Total Number of Passes: {counts['passes']}",
            f"Total Number of Warns: {counts['warns']}",
            f"Total Number of Failures: {counts['fails']}",
            "=" * 60,
        ]

        self.send_to_log(self.INFO, "\n".join(messages))

    def _run(self) -> None:
        """
        This function generates the report based on the predicates.

        :param: None
        :return: None
        """
        self._log_summary()
        for predicate in self.predicates:
            if predicate["has_failed"]:
                level, msg = self._create_fail(predicate)
                self.append_raises(msg, predicate.get("strategy", "fail"))
            else:
                level, msg = self._create_success(predicate)
            self.send_to_log(level, msg)

        self.raise_error()
