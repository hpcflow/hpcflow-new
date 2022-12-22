from datetime import timedelta
from pathlib import Path
from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler, PatternMatchingEventHandler


class MonitorController:
    def __init__(self, workflow_dirs_file_path, watch_interval, logger):

        if isinstance(watch_interval, timedelta):
            watch_interval = watch_interval.total_seconds()

        self.watch_interval = int(watch_interval)
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

        self.observer = PollingObserver(timeout=self.watch_interval)
        self.observer.schedule(
            self.event_handler,
            path=self.workflow_dirs_file_path.parent,
            recursive=False,
        )

        self.observer.start()

        workflow_paths = self.parse_watch_workflows_file(
            self.workflow_dirs_file_path, logger=self.logger
        )
        self.workflow_monitor = WorkflowMonitor(
            workflow_paths,
            watch_interval=self.watch_interval,
            logger=self.logger,
        )

    @staticmethod
    def parse_watch_workflows_file(path, logger):
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
                logger.warning(f"{str(wk_path)} is not a workflow")
                continue

            wks.append(
                {
                    "path": wk_path,
                }
            )

        return wks

    def on_modified(self, event):
        self.logger.info(f"Watch file modified: {event.src_path}")
        wks = self.parse_watch_workflows_file(event.src_path, logger=self.logger)
        self.workflow_monitor.update_workflow_paths(wks)

    def join(self):
        self.observer.join()

    def stop(self):
        self.observer.stop()
        self.observer.join()  # wait for it to stop!
        self.workflow_monitor.stop()


class WorkflowMonitor:
    def __init__(self, workflow_paths, watch_interval, logger):

        if isinstance(watch_interval, timedelta):
            watch_interval = watch_interval.total_seconds()

        self.event_handler = FileSystemEventHandler()
        self.event_handler.on_modified = self.on_modified
        self.workflow_paths = workflow_paths
        self.watch_interval = int(watch_interval)
        self.logger = logger

        self._monitor_workflow_paths()

    def _monitor_workflow_paths(self):

        self.observer = PollingObserver(timeout=self.watch_interval)
        for i in self.workflow_paths:
            self.observer.schedule(self.event_handler, path=i["path"], recursive=False)
            self.logger.info(f"Watching workflow: {i['path'].name}")

        self.observer.start()

    def on_modified(self, event):
        self.logger.info(f"Workflow modified: {event.src_path}")

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
