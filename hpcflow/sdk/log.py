from functools import wraps
import logging
from pathlib import Path
import time
from collections import defaultdict
import statistics


class TimeIt:

    active = False
    file_path = None
    timers = defaultdict(list)
    trace = []
    trace_idx = []
    trace_prev = []
    trace_idx_prev = []

    @classmethod
    def decorator(cls, func):
        @wraps(func)
        def wrapper(*args, **kwargs):

            if not cls.active:
                return func(*args, **kwargs)

            cls.trace.append(func.__qualname__)

            if cls.trace_prev == cls.trace:
                new_trace_idx = cls.trace_idx_prev[-1] + 1
            else:
                new_trace_idx = 0
            cls.trace_idx.append(new_trace_idx)

            tic = time.perf_counter()
            out = func(*args, **kwargs)
            toc = time.perf_counter()
            elapsed = toc - tic

            cls.timers[tuple(cls.trace)].append(elapsed)

            cls.trace_prev = list(cls.trace)
            cls.trace_idx_prev = list(cls.trace_idx)

            cls.trace.pop()
            cls.trace_idx.pop()

            return out

        return wrapper

    @classmethod
    def summarise(cls):
        stats = {
            k: {
                "number": len(v),
                "mean": statistics.mean(v),
                "stddev": statistics.pstdev(v),
                "min": min(v),
                "max": max(v),
                "sum": sum(v),
            }
            for k, v in cls.timers.items()
        }

        # make a graph
        for k in stats:
            stats[k]["children"] = {}

        for key in sorted(stats.keys(), key=lambda x: len(x), reverse=True):
            if len(key) == 1:
                continue
            value = stats.pop(key)
            parent = key[:-1]
            for other_key in stats.keys():
                if other_key == parent:
                    stats[other_key]["children"][key] = value
                    break

        return stats

    @classmethod
    def summarise_string(cls):
        def _format_nodes(node, depth=0, depth_final=None):
            if depth_final is None:
                depth_final = []
            out = []
            for idx, (k, v) in enumerate(node.items()):
                is_final_child = idx == len(node) - 1
                angle = "└ " if is_final_child else "├ "
                bars = ""
                if depth > 0:
                    bars = "".join(f"{'│ ' if not i else '  '}" for i in depth_final)
                k_str = bars + (angle if depth > 0 else "") + f"{k[depth]}"
                number = v["number"]
                min_str = f"{v['min']:10.6f}" if number > 1 else f"{f'-':^12s}"
                max_str = f"{v['max']:10.6f}" if number > 1 else f"{f'-':^12s}"
                stddev_str = f"({v['stddev']:8.6f})" if number > 1 else f"{f' ':^10s}"
                out.append(
                    f"{k_str:.<80s} {v['sum']:12.6f} "
                    f"{v['mean']:10.6f} {stddev_str} {number:8d} "
                    f"{min_str} {max_str} "
                )
                depth_final_next = list(depth_final) + (
                    [is_final_child] if depth > 0 else []
                )
                out.extend(
                    _format_nodes(
                        v["children"], depth=depth + 1, depth_final=depth_final_next
                    )
                )
            return out

        summary = cls.summarise()

        out = [
            f"{'function':^80s} {'sum /s':^12s} {'mean (stddev) /s':^20s} {'N':^8s} "
            f"{'min /s':^12s} {'max /s':^12s}"
        ]
        out += _format_nodes(summary)
        out_str = "\n".join(out)
        if cls.file_path:
            Path(cls.file_path).write_text(out_str, encoding="utf-8")
        else:
            print(out_str)


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

    def remove_file_handlers(self):
        """Remove all file handlers."""
        # TODO: store a `file_handlers` attribute as well as `console_handlers`
        for hdlr in self.logger.handlers:
            if isinstance(hdlr, logging.FileHandler):
                self.logger.debug(f"Removing file handler from the AppLog: {hdlr!r}.")
                self.logger.removeHandler(hdlr)
