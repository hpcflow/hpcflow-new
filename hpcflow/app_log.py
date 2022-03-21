import logging
from pathlib import Path

LOG_LEVELS = list(logging._levelToName.values())


class AppLog:

    DEFAULT_LOG_LEVEL_CONSOLE = "WARNING"
    DEFAULT_LOG_LEVEL_FILE = "INFO"
    DEFAULT_LOG_FILE_PATH = "app.log"

    def __init__(self, package_name, hpcflow_app_log=None):

        self.name = package_name
        self.logger = logging.getLogger(package_name)
        self.logger.setLevel(logging.DEBUG)
        self.hpcflow_app_log = hpcflow_app_log

        self._add_file_logger(AppLog.DEFAULT_LOG_FILE_PATH, AppLog.DEFAULT_LOG_LEVEL_FILE)
        self._add_console_logger(AppLog.DEFAULT_LOG_LEVEL_CONSOLE)

    def get_child_logger(self, name):
        return self.logger.getChild(".".join(name.split(".")[1:]))

    def _add_file_logger(self, filename, level, fmt=None):
        if not fmt:
            fmt = "%(asctime)s %(levelname)s %(name)s: %(message)s"
        handler = logging.FileHandler(filename)
        handler.setFormatter(logging.Formatter(fmt))
        handler.setLevel(level)
        self.logger.addHandler(handler)

    def _add_console_logger(self, level, fmt=None):
        if not fmt:
            fmt = "%(levelname)s %(name)s: %(message)s"
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(fmt))
        handler.setLevel(level)
        self.logger.addHandler(handler)

    def update_handlers(
        self, console_log_level=None, file_log_level=None, file_log_path=None
    ):
        """Modify logging configuration if non-None arguments are passed."""

        stream_hdl = [
            i for i in self.logger.handlers if type(i) is logging.StreamHandler
        ][0]
        file_hdl = [i for i in self.logger.handlers if type(i) is logging.FileHandler][0]

        if console_log_level:
            stream_hdl.setLevel(getattr(logging, console_log_level))

        if file_log_level:
            file_hdl.setLevel(file_log_level)

        if file_log_path:
            file_log_path = Path(file_log_path).resolve()
            self.logger.info(
                f"Now using a new log file path (see you there!): "
                f"{str(file_log_path)!r}"
            )
            self.logger.removeHandler(file_hdl)
            self._add_file_logger(
                file_log_path, file_log_level or AppLog.DEFAULT_LOG_LEVEL_FILE
            )

        if self.hpcflow_app_log:
            # also update hpcflow logger:
            self.hpcflow_app_log.update_handlers(
                console_log_level, file_log_level, file_log_path
            )
