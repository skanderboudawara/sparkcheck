

import pytest

from sparkchecker.bin._report import ReportGenerator
from sparkchecker.ext._exceptions import SparkCheckerError


class TestCreateRuleTable:
    def test_create_rule_table_basic(self) -> None:
        rule = ("test_rule", "col > 0", "True")
        expected = [
            "+-----------+-----------+----------+",
            "| rule      | predicate | expected |",
            "+===========+===========+==========+",
            "| test_rule | col > 0   | True     |",
            "+-----------+-----------+----------+",
        ]
        result = ReportGenerator._create_rule_table(rule)
        assert result == "\n".join(expected)

    def test_create_rule_table_varying_lengths(self) -> None:
        rule = ("short", "very_long_predicate", "med")
        expected = [
            "+-------+---------------------+----------+",
            "| rule  | predicate           | expected |",
            "+=======+=====================+==========+",
            "| short | very_long_predicate | med      |",
            "+-------+---------------------+----------+",
        ]
        result = ReportGenerator._create_rule_table(rule)
        assert result == "\n".join(expected)

    def test_create_rule_table_special_chars(self) -> None:
        rule = ("test!", "@col >= 0", "$True$")
        expected = [
            "+-------+-----------+----------+",
            "| rule  | predicate | expected |",
            "+=======+===========+==========+",
            "| test! | @col >= 0 | $True$   |",
            "+-------+-----------+----------+",
        ]
        result = ReportGenerator._create_rule_table(rule)
        assert result == "\n".join(expected)

    def test_create_rule_table_empty_values(self) -> None:
        rule = ("", "", "")
        expected = [
            "+------+-----------+----------+",
            "| rule | predicate | expected |",
            "+======+===========+==========+",
            "|      |           |          |",
            "+------+-----------+----------+",
        ]
        result = ReportGenerator._create_rule_table(rule)
        assert result == "\n".join(expected)

    def test_create_rule_table_list_input(self) -> None:
        rule = ["rule1", "x < 10", "False"]
        expected = [
            "+-------+-----------+----------+",
            "| rule  | predicate | expected |",
            "+=======+===========+==========+",
            "| rule1 | x < 10    | False    |",
            "+-------+-----------+----------+",
        ]
        result = ReportGenerator._create_rule_table(rule)
        assert result == "\n".join(expected)


class TestReportCreateTable:
    def test_basic_table(self) -> None:
        example = {"column": "value"}
        expected = (
            "+--------+\n"
            "| column |\n"
            "+========+\n"
            "| value  |\n"
            "+--------+"
        )
        result = ReportGenerator._create_table(example)
        assert result == expected

    def test_long_value(self) -> None:
        example = {"col": "very long value"}
        expected = (
            "+-----------------+\n"
            "| col             |\n"
            "+=================+\n"
            "| very long value |\n"
            "+-----------------+"
        )
        assert ReportGenerator._create_table(example) == expected

    def test_special_chars(self) -> None:
        example = {"test": "@#$%"}
        expected = (
            "+------+\n"
            "| test |\n"
            "+======+\n"
            "| @#$% |\n"
            "+------+"
        )
        assert ReportGenerator._create_table(example) == expected

    def test_numeric_value(self) -> None:
        example = {"count": 42}
        expected = (
            "+-------+\n"
            "| count |\n"
            "+=======+\n"
            "| 42    |\n"
            "+-------+"
        )
        assert ReportGenerator._create_table(example) == expected

    def test_empty_value(self) -> None:
        example = {"empty": ""}
        expected = (
            "+-------+\n"
            "| empty |\n"
            "+=======+\n"
            "|       |\n"
            "+-------+"
        )
        assert ReportGenerator._create_table(example) == expected


class TestCreateFail:

    @pytest.fixture
    def patched_instance(self, monkeypatch):
        def noop_init(self, logger, predicates, raise_error) -> None:
            pass

        monkeypatch.setattr(ReportGenerator, "__init__", noop_init)

        # Instantiate the class
        report_generator = ReportGenerator(None, None, None)
        return report_generator

    def test_create_warn_basic(self, patched_instance) -> None:
        predicate = {
            "check": "column",
            "has_failed": True,
            "strategy": "warn",
            "value": "arrival_country",
            "got": 20480,
            "operator": "different",
            "message": "ColCompareCheck: The passenger country should be different from the arrival country in this dataset",
            "column": "passengers_country",
            "rule": ("passengers_country", "different", "arrival_country"),
            "example": {"passengers_country": "USA"},
        }
        expected_msg = [
            "⚠️ WARN - Rule has failed",
            "Rule:",
            "+--------------------+-----------+-----------------+",
            "| rule               | predicate | expected        |",
            "+====================+===========+=================+",
            "| passengers_country | different | arrival_country |",
            "+--------------------+-----------+-----------------+",
            "ColCompareCheck: The passenger country should be different from the arrival country in this dataset",
            "Number of rows failed: 20480",
            "Example:",
            "+--------------------+",
            "| passengers_country |",
            "+====================+",
            "| USA                |",
            "+--------------------+",
        ]
        log_level, msg = patched_instance._create_fail(predicate)
        assert msg == "\n".join(expected_msg)
        assert log_level == 30

    def test_create_fail_basic(self, patched_instance) -> None:
        predicate = {
            "check": "column",
            "has_failed": True,
            "strategy": "fail",
            "value": "arrival_country",
            "got": 20480,
            "operator": "different",
            "message": "ColCompareCheck: The passenger country should be different from the arrival country in this dataset",
            "column": "passengers_country",
            "rule": ("passengers_country", "different", "arrival_country"),
            "example": {"passengers_country": "USA"},
        }
        expected_msg = [
            "❌ FAIL - Rule has failed",
            "Rule:",
            "+--------------------+-----------+-----------------+",
            "| rule               | predicate | expected        |",
            "+====================+===========+=================+",
            "| passengers_country | different | arrival_country |",
            "+--------------------+-----------+-----------------+",
            "ColCompareCheck: The passenger country should be different from the arrival country in this dataset",
            "Number of rows failed: 20480",
            "Example:",
            "+--------------------+",
            "| passengers_country |",
            "+====================+",
            "| USA                |",
            "+--------------------+",
        ]
        log_level, msg = patched_instance._create_fail(predicate)
        assert msg == "\n".join(expected_msg)
        assert log_level == 40

    def test_create_fail_no_example(self, patched_instance) -> None:
        predicate = {
            "check": "column",
            "has_failed": True,
            "strategy": "fail",
            "value": "arrival_country",
            "got": 20480,
            "operator": "different",
            "message": "ColCompareCheck: The passenger country should be different from the arrival country in this dataset",
            "column": "passengers_country",
            "rule": ("passengers_country", "different", "arrival_country"),
        }
        expected_msg = [
            "❌ FAIL - Rule has failed",
            "Rule:",
            "+--------------------+-----------+-----------------+",
            "| rule               | predicate | expected        |",
            "+====================+===========+=================+",
            "| passengers_country | different | arrival_country |",
            "+--------------------+-----------+-----------------+",
            "ColCompareCheck: The passenger country should be different from the arrival country in this dataset",
            "Number of rows failed: 20480",
        ]
        log_level, msg = patched_instance._create_fail(predicate)
        assert msg == "\n".join(expected_msg)
        assert log_level == 40


class TestCreateSuccess:

    @pytest.fixture
    def patched_instance(self, monkeypatch):
        def noop_init(self, logger, predicates, raise_error) -> None:
            pass

        monkeypatch.setattr(ReportGenerator, "__init__", noop_init)

        # Instantiate the class
        report_generator = ReportGenerator(None, None, None)
        return report_generator

    def test_create_success_basic(self, patched_instance) -> None:
        predicate = {
            "check": "column",
            "has_failed": False,
            "strategy": "warn",
            "value": "arrival_country",
            "got": 0,
            "operator": "different",
            "message": "ColCompareCheck: The passenger country is ok",
            "column": "passengers_country",
            "rule": ("passengers_country", "different", "arrival_country"),
        }
        expected_msg = [
            "✅ PASS - Rule has succeeded",
            "Rule:",
            "+--------------------+-----------+-----------------+",
            "| rule               | predicate | expected        |",
            "+====================+===========+=================+",
            "| passengers_country | different | arrival_country |",
            "+--------------------+-----------+-----------------+",
        ]
        log_level, msg = patched_instance._create_success(predicate)
        assert msg == "\n".join(expected_msg)
        assert log_level == 20


class TestAppendRaise:

    @pytest.fixture
    def patched_instance(self, monkeypatch):
        def noop_run(self) -> None:
            pass

        monkeypatch.setattr(ReportGenerator, "_run", noop_run)

        # Instantiate the class
        report_generator = ReportGenerator("a", ["b"], False)
        return report_generator

    @pytest.mark.parametrize(
        ("msg", "strategy", "expected"),
        [
            ("hello fail", "fail", ["hello fail"]),
            ("hello warn", "warn", []),
            ("hello info", "info", []),
        ],
    )
    def test_append_raise(self, patched_instance, msg, strategy, expected) -> None:
        patched_instance.append_raises(msg, strategy)
        assert patched_instance.raises == expected


class TestReportRaise:
    @pytest.fixture
    def patched_instance(self, monkeypatch) -> int:
        def noop_run(self) -> None:
            pass

        monkeypatch.setattr(ReportGenerator, "_run", noop_run)
        return 1

    @pytest.mark.parametrize(
        ("raises", "raise_error", "should_raise"),
        [
            (["hello"], False, False),
            (["hello"], True, True),
            ([], True, False),
            ([], False, False),
        ],
    )
    def test_raise_error(self, patched_instance, raises, raise_error, should_raise) -> None:
        report_generator = ReportGenerator("a", ["b"], raise_error)
        report_generator.raises = raises
        if should_raise:
            with pytest.raises(SparkCheckerError):
                report_generator.raise_error()
        else:
            report_generator.raise_error()


class TestLogSummary:
    @pytest.fixture
    def patched_instance(self, monkeypatch) -> int:
        def noop_run(self) -> None:
            pass

        monkeypatch.setattr(ReportGenerator, "_run", noop_run)
        return 1

    @pytest.mark.parametrize(
        ("predicates", "expected_message", "expected_level"),
        [
            (
                [
                    {"has_failed": True, "strategy": "warn"},
                    {"has_failed": True, "strategy": "fail"},
                    {"has_failed": True, "strategy": "warn"},
                    {"has_failed": False, "strategy": "warn"},
                    {"has_failed": False},
                    {"has_failed": True},
                ],
                [
                    "Total Number of Checks: 6",
                    "Total Number of Passes: 2",
                    "Total Number of Warns: 2",
                    "Total Number of Failures: 2",
                    "============================================================",
                ],
                20,
            ),
            (
                [
                    {"has_failed": False, "strategy": "warn"},
                    {"has_failed": False, "strategy": "fail"},
                    {"has_failed": False, "strategy": "warn"},
                    {"has_failed": False, "strategy": "warn"},
                    {"has_failed": False},
                    {"has_failed": False},
                ],
                [
                    "Total Number of Checks: 6",
                    "Total Number of Passes: 6",
                    "Total Number of Warns: 0",
                    "Total Number of Failures: 0",
                    "============================================================",
                ],
                20,
            ),
            (
                [
                    {"has_failed": True, "strategy": "warn"},
                    {"has_failed": True, "strategy": "fail"},
                    {"has_failed": True, "strategy": "warn"},
                    {"has_failed": True, "strategy": "warn"},
                    {"has_failed": True},
                    {"has_failed": True},
                ],
                [
                    "Total Number of Checks: 6",
                    "Total Number of Passes: 0",
                    "Total Number of Warns: 3",
                    "Total Number of Failures: 3",
                    "============================================================",
                ],
                20,
            ),
        ],
    )
    def test_log_summary(self, patched_instance, predicates, expected_message, expected_level) -> None:
        report_generator = ReportGenerator("a", predicates, True)
        lvl, msg = report_generator._log_summary()
        assert msg == "\n".join(expected_message)
        assert lvl == expected_level


class TestSendToLog:
    @pytest.fixture
    def mock_logger(self, monkeypatch) -> int:
        class MockLogger:
            def __init__(self) -> None:
                self.log_calls = []

            def log(self, level, msg, *args, **kwargs) -> None:
                self.log_calls.append((level, msg))

        mock_logger = MockLogger()

        def mock_init(self, logger, predicates, raise_error) -> None:
            self.logger = mock_logger
            self.predicates = predicates
            self._raise_error = raise_error
            self.raises = []

        monkeypatch.setattr(ReportGenerator, "__init__", mock_init)
        return 1

    @pytest.mark.parametrize(
        ("level", "expected_level"),
        [
            (10, 10),
            (20, 20),
            (30, 30),
            (40, 40),
            (50, 50),
        ],
    )
    def test_info_log(self, mock_logger, level, expected_level) -> None:
        report_generator = ReportGenerator("a", ["b"], True)
        msg = "Test info message"
        report_generator.send_to_log(level, msg)
        assert report_generator.logger.log_calls == [(expected_level, "Test info message")]


class TestRun:
    @pytest.fixture
    def mock_logger(self, monkeypatch) -> int:
        class MockLogger:
            def __init__(self) -> None:
                self.log_calls = []

            def log(self, level, msg, *args, **kwargs) -> None:
                self.log_calls.append((level, msg))

        mock_logger = MockLogger()

        def mock_init(self, logger, predicates, raise_error) -> None:
            self.logger = mock_logger
            self.predicates = predicates
            self._raise_error = raise_error
            self.raises = []

        def mock_raise_error(self) -> None:
            pass

        monkeypatch.setattr(ReportGenerator, "__init__", mock_init)
        monkeypatch.setattr(ReportGenerator, "raise_error", mock_raise_error)
        return 1

    def test_run(self, mock_logger) -> None:
        predicates = [
            {
                "check": "column",
                "has_failed": False,
                "strategy": "warn",
                "value": "arrival_country",
                "got": 0,
                "operator": "different",
                "message": "ColCompareCheck: The arrival_country is ok",
                "column": "passengers_country",
                "rule": ("passengers_country", "different", "arrival_country"),
            },
            {
                "check": "column",
                "has_failed": True,
                "strategy": "warn",
                "value": "arrival_country2",
                "got": 2,
                "operator": "different",
                "message": "ColCompareCheck: The arrival_country2 is not ok",
                "column": "passengers_country",
                "rule": ("passengers_country", "different", "arrival_country2"),
                "example": {"passengers_country": "USA"},
            },
            {
                "check": "column",
                "has_failed": True,
                "strategy": "fail",
                "value": "arrival_country3",
                "got": 3,
                "operator": "different",
                "message": "ColCompareCheck: The arrival_country3 is not ok",
                "column": "passengers_country",
                "rule": ("passengers_country", "different", "arrival_country3"),
                "example": {"passengers_country": "USA"},
            },
            {
                "check": "column",
                "has_failed": True,
                "value": "arrival_country4",
                "got": 4,
                "operator": "different",
                "message": "ColCompareCheck: The arrival_country4 is not ok",
                "column": "passengers_country",
                "rule": ("passengers_country", "different", "arrival_country4"),
                "example": {"passengers_country": "USA"},
            },
        ]
        report_generator = ReportGenerator("a", predicates, True)
        report_generator._run()
        assert report_generator.raises == [
            "❌ FAIL - Rule has failed\nRule:\n+--------------------+-----------+------------------+\n| rule               | predicate | expected         |\n+====================+===========+==================+\n| passengers_country | different | arrival_country3 |\n+--------------------+-----------+------------------+\nColCompareCheck: The arrival_country3 is not ok\nNumber of rows failed: 3\nExample:\n+--------------------+\n| passengers_country |\n+====================+\n| USA                |\n+--------------------+",
            "❌ FAIL - Rule has failed\nRule:\n+--------------------+-----------+------------------+\n| rule               | predicate | expected         |\n+====================+===========+==================+\n| passengers_country | different | arrival_country4 |\n+--------------------+-----------+------------------+\nColCompareCheck: The arrival_country4 is not ok\nNumber of rows failed: 4\nExample:\n+--------------------+\n| passengers_country |\n+====================+\n| USA                |\n+--------------------+",
        ]
        assert report_generator.logger.log_calls == [
            (20, "Total Number of Checks: 4\nTotal Number of Passes: 1\nTotal Number of Warns: 1\nTotal Number of Failures: 2\n============================================================"),
            (20, "✅ PASS - Rule has succeeded\nRule:\n+--------------------+-----------+-----------------+\n| rule               | predicate | expected        |\n+====================+===========+=================+\n| passengers_country | different | arrival_country |\n+--------------------+-----------+-----------------+"),
            (30, "⚠️ WARN - Rule has failed\nRule:\n+--------------------+-----------+------------------+\n| rule               | predicate | expected         |\n+====================+===========+==================+\n| passengers_country | different | arrival_country2 |\n+--------------------+-----------+------------------+\nColCompareCheck: The arrival_country2 is not ok\nNumber of rows failed: 2\nExample:\n+--------------------+\n| passengers_country |\n+====================+\n| USA                |\n+--------------------+"),
            (40, "❌ FAIL - Rule has failed\nRule:\n+--------------------+-----------+------------------+\n| rule               | predicate | expected         |\n+====================+===========+==================+\n| passengers_country | different | arrival_country3 |\n+--------------------+-----------+------------------+\nColCompareCheck: The arrival_country3 is not ok\nNumber of rows failed: 3\nExample:\n+--------------------+\n| passengers_country |\n+====================+\n| USA                |\n+--------------------+"),
            (40, "❌ FAIL - Rule has failed\nRule:\n+--------------------+-----------+------------------+\n| rule               | predicate | expected         |\n+====================+===========+==================+\n| passengers_country | different | arrival_country4 |\n+--------------------+-----------+------------------+\nColCompareCheck: The arrival_country4 is not ok\nNumber of rows failed: 4\nExample:\n+--------------------+\n| passengers_country |\n+====================+\n| USA                |\n+--------------------+"),
        ]
