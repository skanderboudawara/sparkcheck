# from sparkchecker.bin._exceptions import IllegalCheckStrategy


# class LogAndRaise:
#     """A class that logs a message and raises an exception."""

#     def __init__(self, checks) -> None:
#         self.checks = checks

#     def process_log(self) -> None:
#         for check in self.checks:
#             if check["strategy"] not in {"fail", "warn"}:
#                 raise IllegalCheckStrategy(check)
#             if check["strategy"] == "failed":
#                 logger_expectations.error(
#                     f"Check {check['name']} failed: {check['message']}",
#                 )
#                 raise Exception(f"Check {check['name']} failed: {check['message']}")
#             logger_expectations.info(f"Check {check['name']} passed.")
