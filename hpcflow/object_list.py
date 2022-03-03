class DotAccessObjectList:
    def __init__(self, *objects, access_attribute, descriptor):
        self._objects = list(objects)
        self._access_attribute = access_attribute
        self._descriptor = descriptor

        if self._objects:

            if not hasattr(self._objects[0], self._access_attribute):
                raise TypeError(
                    f"Objects do not have attribute {self._access_attribute!r}."
                )

    def __len__(self):
        return len(self._objects)

    def __repr__(self):
        return repr(self._objects)

    def __str__(self):
        return str(self._objects)

    def __getitem__(self, key):
        return self._objects.__getitem__(key)

    def __iter__(self):
        return self._objects.__iter__()

    def __contains__(self, item):
        return self._objects.__contains__(item)

    def __eq__(self, other):
        return self._objects == other

    def __getattr__(self, attribute):
        for obj in self._objects:
            if getattr(obj, self._access_attribute) == attribute:
                return obj

        obj_list_fmt = ", ".join(
            [f'"{getattr(i, self._access_attribute)}"' for i in self._objects]
        )
        msg = (
            f"{self._descriptor.title()} {attribute!r} does not exist. Available "
            f"{self._descriptor}s are: {obj_list_fmt}."
        )
        raise AttributeError(msg)

    def __dir__(self):
        return super().__dir__() + [
            getattr(i, self._access_attribute) for i in self._objects
        ]

    def add_object(self, obj, index=-1):
        if not hasattr(obj, self._access_attribute):
            raise TypeError(
                f"Object does not have attribute {self._access_attribute!r}."
            )
        if index < 0:
            index += len(self) + 1
        self._objects = self._objects[:index] + [obj] + self._objects[index:]


class TaskList(DotAccessObjectList):
    """A list-like container for a task-like list with dot-notation access by task
    unique-name."""

    def __init__(self, *task_likes):
        super().__init__(
            *task_likes, access_attribute="unique_name", descriptor="task-like"
        )


class GroupList(DotAccessObjectList):
    """A list-like container for the task schema group list with dot-notation access by
    group name."""

    def __init__(self, *groups):
        super().__init__(*groups, access_attribute="name", descriptor="group")


class ExecutablesList(DotAccessObjectList):
    """A list-like container for environment executables with dot-notation access by
    executable label."""

    def __init__(self, *execs):
        super().__init__(*execs, access_attribute="label", descriptor="executable")


def index(obj_lst, obj):
    for idx, i in enumerate(obj_lst._objects):
        if obj is i:
            return idx
    raise ValueError(f"{obj!r} not in list.")
