from __future__ import annotations
import copy
from dataclasses import dataclass, field
from typing import Dict, List
from datetime import datetime

from colorama import init as colorama_init
from termcolor import colored
import numcodecs
import numpy as np

from .utils import get_md5_hash, remove_ansi_escape_sequences


@dataclass
class EventLogItem:

    event_log: EventLog = field(repr=False)
    index: int
    time: np.datetime64
    event_type: str
    machine: str
    content: Dict
    depth: int
    parent: int

    def __post_init__(self):
        if self.parent == -1:
            self.parent = None


class EventLog:
    """Class to track how a workflow was generated."""

    _metadata_dtype = np.dtype(
        {
            "names": [
                "index",
                "time",
                "event_type",
                "machine",
                "depth",
                "parent",
            ],
            "formats": ["int8", "M8[us]", "int8", "int8", "int8", "int8"],
        }
    )

    def __init__(self, workflow):
        colorama_init(autoreset=True)

        self.workflow = workflow

        self._set_original_metadata_lookup()
        self._metadata_lookup = copy.deepcopy(self._original_metadata_lookup)

        disk_mdl = self._get_zarr_group("r").attrs["metadata_lookup"]

        self._new_event_metadata = []
        self._new_event_content = []

    def __eq__(self, other):

        if np.all(self.metadata[:] == other.metadata[:]):
            if np.all(self.content[:] == other.content[:]):
                if self._metadata_lookup == other._metadata_lookup:
                    return True

        return False

    def _set_original_metadata_lookup(self):
        self._original_metadata_lookup = self._get_zarr_group("r").attrs[
            "metadata_lookup"
        ]

    def _check_metadata_lookup_changed(self):
        h1 = get_md5_hash(self._metadata_lookup)
        h2 = get_md5_hash(self._original_metadata_lookup)
        return h1 != h2

    def __len__(self):
        return len(self.metadata)

    def _get_zarr_group(self, mode="r"):
        return self.workflow._get_workflow_root_group(mode).event_log

    @property
    def metadata(self):
        return self._get_zarr_group().metadata

    @property
    def content(self):
        return self._get_zarr_group().content

    @classmethod
    def generate_in(cls, zarr_group):
        """Generate the persistent data to represent a new event log in a new workflow."""
        event_log = zarr_group.create_group("event_log")
        event_log.attrs["metadata_lookup"] = {
            "event_type": [],
            "machine": [],
        }
        event_log.empty(
            name="metadata",
            dtype=cls._metadata_dtype,
            shape=0,
        )
        event_log.empty(
            name="content",
            dtype=object,
            shape=0,
            object_codec=numcodecs.MsgPack(),
        )

    def _update_metadata_lookup(self, key, item):

        if item in self._metadata_lookup[key]:
            return self._metadata_lookup[key].index(item)
        else:
            self._metadata_lookup[key].append(item)
            return len(self._metadata_lookup[key]) - 1

    @property
    def num_saved_events(self):
        return len(self.metadata)

    @property
    def num_events(self):
        return self.num_saved_events + len(self._new_event_metadata)

    @property
    def has_unsaved_events(self):
        return len(self._new_event_metadata) > 0

    @staticmethod
    def get_timestamp():
        return datetime.utcnow()

    def dump_new_events(self):
        """"""
        if not self._new_event_metadata:
            # no changes
            return

        zarr_group = self._get_zarr_group(mode="r+")

        if self._check_metadata_lookup_changed():
            zarr_group.attrs.put({"metadata_lookup": self._metadata_lookup})
            self._set_original_metadata_lookup()

        zarr_group.metadata.append(np.concatenate(self._new_event_metadata))
        zarr_group.content.append(self._new_event_content)

        self._new_event_metadata = []
        self._new_event_content = []

    def discard_new_events(self):

        self._new_event_metadata = []
        self._new_event_content = []

        if self._check_metadata_lookup_changed():
            self._metadata_lookup = copy.deepcopy(self._original_metadata_lookup)

    def _save_new_events(self):
        if not self.workflow._in_batch_mode:
            self.dump_new_events()

    def _add_event(
        self,
        event_type,
        timestamp=None,
        machine=None,
        parents=None,
        content=None,
    ):

        # TODO: log to app log.

        evt_time = timestamp or self.get_timestamp()

        evt_machine = machine or self.workflow.app.config.get("machine")
        evt_machine_idx = self._update_metadata_lookup("machine", evt_machine)
        evt_type_idx = self._update_metadata_lookup("event_type", event_type)
        evt_idx = self.num_events

        evt_depth = len(parents) if parents else 0
        evt_parent = parents[-1] if parents else -1

        new_content = content or {}
        new_metadata = np.array(
            [
                (
                    evt_idx,
                    evt_time,
                    evt_type_idx,
                    evt_machine_idx,
                    evt_depth,
                    evt_parent,
                )
            ],
            dtype=self._metadata_dtype,
        )

        self._new_event_metadata.append(new_metadata)
        self._new_event_content.append(new_content)

        self._save_new_events()
        evt = self._construct_event(evt_content=new_content, evt_meta=new_metadata[0])

        return evt

    def event_workflow_create(self, timestamp, machine):
        return self._add_event(
            event_type="create_workflow",
            timestamp=timestamp,
            machine=machine,
        )

    def event_add_task(self, new_task_index, **kwargs):
        return self._add_event(
            event_type="add_task",
            content={"new_task_index": new_task_index},
            **kwargs,
        )

    def event_add_empty_task(
        self, new_task_index, added_shared_data, new_parameter_groups, **kwargs
    ):
        return self._add_event(
            event_type="add_empty_task",
            content={
                "new_task_index": new_task_index,
                "added_shared_data": added_shared_data,
                "new_parameter_groups": new_parameter_groups,
            },
            **kwargs,
        )

    def event_add_elements(self, task_index, **kwargs):
        return self._add_event(
            event_type="add_elements",
            content={
                "task_index": task_index,
            },
            **kwargs,
        )

    def event_add_element_set(
        self, task_index, new_element_indices, new_parameter_groups, **kwargs
    ):
        return self._add_event(
            event_type="add_element_set",
            content={
                "task_index": task_index,
                "new_element_indices": new_element_indices,
                "new_parameter_groups": new_parameter_groups,
            },
            **kwargs,
        )

    def event_remove_task(self, task_index, reason, **kwargs):
        return self._add_event(
            event_type="remove_task",
            content={
                "task_index": task_index,
                "reason": reason,
            },
            **kwargs,
        )

    def _construct_event(self, evt_content, evt_meta, meta_fields=None, as_json=False):
        meta_fields = meta_fields or self.metadata.dtype.fields.keys()
        evt = {}
        for i, j in zip(meta_fields, evt_meta):
            if i in self._metadata_lookup:
                j = self._metadata_lookup[i][j]
            evt[i] = j
        evt.update(content=evt_content)
        if not as_json:
            evt = EventLogItem(**evt, event_log=self)
        return evt

    def get_events(self, event_slice=None, as_json=False):

        if not event_slice:
            event_slice = slice(None)  # get all events

        out = []
        meta_fields = self.metadata.dtype.fields.keys()

        # first get events from the persistent store (using slices on the zarr arrays):
        for evt_content, evt_meta in zip(
            self.content[event_slice], self.metadata[event_slice]
        ):
            out.append(self._construct_event(evt_content, evt_meta, meta_fields, as_json))

        # then add any remaining slice indices from the pending updates:
        for evt_idx in range(self.num_events)[event_slice]:
            if evt_idx in range(self.num_saved_events):
                continue
            evt_content = self._new_event_content[evt_idx - self.num_saved_events]
            evt_meta = self._new_event_metadata[evt_idx - self.num_saved_events][0]
            out.append(self._construct_event(evt_content, evt_meta, meta_fields, as_json))

        return out

    def get_event(self, event_index, as_json=False):
        if event_index < 0:
            event_index = self.num_events + event_index
        return self.get_events(
            event_slice=slice(event_index, event_index + 1), as_json=as_json
        )[0]

    def get_events_of_type(self, event_type, as_json=False):
        type_idx = self._metadata_lookup["event_type"].index(event_type)
        idx = np.where(self.metadata["event_type"] == type_idx)[0]
        return [self.get_event(i, as_json=as_json) for i in idx]

    def format_events(self, event_slice=None, full=False, max_line_length=90):
        events = self.get_events(event_slice)

        num_events = len(self)
        idx_size = len(str(num_events))
        out = []
        for evt in events:
            if not full:
                evt_time = np.datetime_as_string(evt.time, unit="s").replace("T", " ")
                parent = f"[{evt.parent:>{idx_size}}] " if evt.parent is not None else ""
                if parent:
                    parent = (("-" * idx_size) + "--") * evt.depth + parent
                out.append(
                    f"{evt.index:>{idx_size}} {evt_time} ({evt.machine}): "
                    f"{parent}{evt.event_type}"
                )
            else:
                evt_time = np.datetime_as_string(evt.time, unit="us").replace("T", " ")
                out.append(f"index:   {evt.index}")
                out.append(f"parent:  {evt.parent}")
                out.append(f"depth:   {evt.depth}")
                out.append(f"time:    {evt.time}")
                out.append(f"type:    {evt.event_type}")
                out.append(f"machine: {evt.machine}")
                if evt.content:
                    out.append("content:")
                    for k, v in evt.content.items():
                        out.append(f"  {k}: {v}")

                out.append("")

        out_str = ""
        for ln in out:
            if len(ln) > max_line_length:
                # remove ansi escape sequences, since we might truncate them:
                ln = remove_ansi_escape_sequences(ln)
                ln = ln[: max_line_length - 3] + ("...")

            out_str += ln + "\n"

        return out_str
