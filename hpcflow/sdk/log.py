import logging


class AppLog:
    DEFAULT_LOG_CONSOLE_LEVEL = "WARNING"
    DEFAULT_LOG_FILE_LEVEL = "INFO"

    def __init__(self, app, log_console_level=None):
        self.app = app
        self.logger = logging.getLogger(app.package_name)
        self.logger.setLevel(logging.DEBUG)
        self.console_handler = self._add_console_logger(
            level=log_console_level or AppLog.DEFAULT_LOG_CONSOLE_LEVEL
        )

    def _add_console_logger(self, level, fmt=None):
        fmt = fmt or "%(levelname)s %(name)s: %(message)s"
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(fmt))
        handler.setLevel(level)
        self.logger.addHandler(handler)
        return handler

    def update_console_level(self, new_level):
        if new_level:
            self.console_handler.setLevel(new_level.upper())

    def add_file_logger(self, path, level=None, fmt=None, max_bytes=None):
        fmt = fmt or f"%(asctime)s %(levelname)s %(name)s: %(message)s"
        level = level or AppLog.DEFAULT_LOG_FILE_LEVEL
        max_bytes = max_bytes or int(10e6)

        if not path.parent.is_dir():
            self.logger.info(f"Generating log file parent directory: {path.parent!r}")
            path.parent.mkdir(exist_ok=True, parents=True)

        handler = logging.handlers.RotatingFileHandler(filename=path, maxBytes=max_bytes)
        handler.setFormatter(logging.Formatter(fmt))
        handler.setLevel(level.upper())
        self.logger.addHandler(handler)
        return handler
