import logging


class AppLog:

    DEFAULT_LOG_LEVEL_CONSOLE = "WARNING"
    DEFAULT_LOG_LEVEL_FILE = "INFO"
    DEFAULT_LOG_FILE_PATH = "app.log"

    def __init__(self, app, log_console_level=None):
        self.app = app
        self.logger = logging.getLogger(app.name)
        self.logger.setLevel(logging.DEBUG)
        self.console_handler = self._add_console_logger(
            level=log_console_level or AppLog.DEFAULT_LOG_LEVEL_CONSOLE
        )
        # print(f"log_console_level: {log_console_level}")

    def _add_console_logger(self, level, fmt=None):
        if not fmt:
            fmt = "%(levelname)s %(name)s: %(message)s"
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(fmt))
        handler.setLevel(level)
        self.logger.addHandler(handler)
        return handler
