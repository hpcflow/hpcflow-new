import time
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, PatternMatchingEventHandler

logger = None


class MonitorController:
    def __init__(self, workflow_dirs_file_path, logger):

        self.workflow_dirs_file_path = Path(workflow_dirs_file_path).absolute()
        self.logger = logger

        if not self.workflow_dirs_file_path.exists():
            self.logger.info(
                f"Watch file does not exist; creating {str(self.workflow_dirs_file_path)}."
            )
            with self.workflow_dirs_file_path.open("wt") as fp:
                fp.write("\n")

        self.logger.info(f"Watching file: {str(self.workflow_dirs_file_path)}")

        self.event_handler = PatternMatchingEventHandler(patterns=["watch_workflows.txt"])
        self.event_handler.on_modified = self.on_modified

        self.observer = Observer()
        self.observer.schedule(
            self.event_handler,
            path=self.workflow_dirs_file_path.parent,
            recursive=False,
        )

        self.observer.start()

        workflow_paths = self.parse_watch_workflows_file(self.workflow_dirs_file_path)
        self.workflow_monitor = WorkflowMonitor(workflow_paths, logger=self.logger)

    def parse_watch_workflows_file(self, path):
        # TODO: and parse element IDs as well; and record which are set/unset.
        with Path(path).open("rt") as fp:
            lns = fp.readlines()

        wks = []
        for ln in lns:
            ln_s = ln.strip()
            if not ln_s:
                continue
            wk_path = Path(ln_s).absolute()
            if not wk_path.is_dir():
                self.logger.warning(f"{str(wk_path)} is not a workflow")
                continue

            wks.append(
                {
                    "path": wk_path,
                }
            )

        return wks

    def on_modified(self, event):
        self.logger.info(f"Watch file modified: {event.src_path}")
        wks = self.parse_watch_workflows_file(event.src_path)
        self.workflow_monitor.update_workflow_paths(wks)

    def join(self):
        self.observer.join()

    def stop(self):
        self.observer.stop()
        self.observer.join()  # wait for it to stop!
        self.workflow_monitor.stop()


class WorkflowMonitor:
    def __init__(self, workflow_paths, logger):

        self.event_handler = FileSystemEventHandler()
        self.workflow_paths = workflow_paths
        self.logger = logger

        self._monitor_workflow_paths()

    def _monitor_workflow_paths(self):

        self.observer = Observer()
        for i in self.workflow_paths:
            self.observer.schedule(self.event_handler, path=i["path"], recursive=False)
            self.logger.info(f"Watching workflow: {i['path'].name}")

        self.observer.start()

    def update_workflow_paths(self, new_paths):
        self.logger.info(f"Updating watched workflows.")
        self.stop()
        self.workflow_paths = new_paths
        self._monitor_workflow_paths()

    def stop(self):
        if self.observer:
            self.observer.stop()
            self.observer.join()  # wait for it to stop!
            self.observer = None
