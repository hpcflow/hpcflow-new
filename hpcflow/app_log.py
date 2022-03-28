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

        self.console_handler = self._add_console_logger(AppLog.DEFAULT_LOG_LEVEL_CONSOLE)
        self.file_handler = None
        self.set_from_CLI = {
            "log_console_level": False,
            "log_file_level": False,
            "log_file_path": False,
        }

    def add_file_logger(self, path, level=None, fmt=None):
        self.file_handler = self._add_file_logger(Path(path), level, fmt)

    def _add_file_logger(self, path=None, level=None, fmt=None):
        log_dir = Path(path.parents[0])
        log_dir.mkdir(exist_ok=True)
        if not fmt:
            fmt = "%(asctime)s %(levelname)s %(name)s: %(message)s"
        handler = logging.FileHandler(path)
        handler.setFormatter(logging.Formatter(fmt))
        handler.setLevel(level or AppLog.DEFAULT_LOG_LEVEL_FILE)
        self.logger.addHandler(handler)

    def _add_console_logger(self, level, fmt=None):
        if not fmt:
            fmt = "%(levelname)s %(name)s: %(message)s"
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(fmt))
        handler.setLevel(level)
        self.logger.addHandler(handler)
        return handler

    def _update_set_from_CLI(
        self,
        log_console_level=None,
        log_file_level=None,
        log_file_path=None,
        from_CLI=False,
    ):
        if log_console_level and self.set_from_CLI["log_console_level"]:
            log_console_level = None
            self.logger.info(
                f"Disregarding change to console log level because this has been set by "
                f"a CLI option."
            )

        if log_file_level and self.set_from_CLI["log_file_level"]:
            log_file_level = None
            self.logger.info(
                f"Disregarding change to log file level because this has been set by a "
                f"CLI option."
            )

        if log_file_path and self.set_from_CLI["log_file_path"]:
            log_file_path = None
            self.logger.info(
                f"Disregarding change to log file path because this has been set by a CLI "
                f"option."
            )

        self.set_from_CLI = {
            "log_console_level": (log_console_level and from_CLI)
            or self.set_from_CLI["log_console_level"],
            "log_file_level": (log_file_level and from_CLI)
            or self.set_from_CLI["log_file_level"],
            "log_file_path": (log_file_path and from_CLI)
            or self.set_from_CLI["log_file_path"],
        }

        return (log_console_level, log_file_level, log_file_path)

    def update_handlers(
        self,
        log_console_level=None,
        log_file_level=None,
        log_file_path=None,
        from_CLI=False,
    ):
        """Modify logging configuration if non-None arguments are passed.

        Parameters
        ----------
        from_CLI
            If True, disallow future modifications of the specified parameters. This is to
            allow the CLI passed parameters to override those specified in the config
            file.
        """

        (log_console_level, log_file_level, log_file_path) = self._update_set_from_CLI(
            log_console_level, log_file_level, log_file_path, from_CLI
        )

        if log_console_level:
            self.console_handler.setLevel(getattr(logging, log_console_level))

        if log_file_level or log_file_path:

            new_file_path = log_file_path or AppLog.DEFAULT_LOG_FILE_PATH
            new_file_level = log_file_level or AppLog.DEFAULT_LOG_LEVEL_FILE

            cur_file_path = None
            if self.file_handler:
                cur_file_path = Path(self.file_handler.baseFilename)

                if log_file_level:
                    self.file_handler.setLevel(log_file_level)

                if log_file_path:
                    self.logger.info(f"Now using a new log file path: {log_file_path!r}")
                    self.logger.removeHandler(self.file_handler)
                    self.file_handler = None

            if log_file_path:
                self.add_file_logger(path=new_file_path, level=new_file_level)
                if cur_file_path:
                    self.logger.info(
                        f"Log file path changed from the previous path: {cur_file_path!r}."
                    )

        if self.hpcflow_app_log:
            # also update hpcflow logger:
            self.hpcflow_app_log.update_handlers(
                log_console_level, log_file_level, log_file_path, from_CLI
            )
