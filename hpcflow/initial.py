from dataclasses import dataclass
from typing import List, Optional


@dataclass
class LoadFromCSVFileSchema(TaskSchema):

    objective: Optional[str] = "load_from_CSV_file"


@dataclass
class BoundTaskTemplate:
    """A TaskTemplate, as bound within a WorkflowTemplate."""

    task_template: TaskTemplate
    workflow_template: WorkflowTemplate
    element_indices: List
    name_repeat_index: int

    @property
    def index(self):
        """Zero-based index of this task within the workflow."""
        return self.workflow_template.tasks.index(self)

    @property
    def unique_name(self):
        add_rep = f"_{self.name_repeat_index if self.name_repeat_index > 1 else ''}"
        return f"{self.task_template.unique_name}{add_rep}"
