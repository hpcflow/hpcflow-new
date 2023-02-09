import copy
from types import SimpleNamespace

from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike


class ObjectList(JSONLike):
    """A list-like class that provides item access via a `get` method according to
    attributes or dict-keys.

    """

    def __init__(self, objects, descriptor=None):
        """

        Parameters
        ----------
        objects : sequence
            List
        access_attribute : str
            Name of the attribute through which objects are accessed. The values must be
            hashable.
        descriptor : str

        """

        self._objects = list(objects)
        self._descriptor = descriptor or "object"
        self._object_is_dict = False

        self._validate()

    def __deepcopy__(self, memo):
        obj = self.__class__(copy.deepcopy(self._objects, memo))
        obj._descriptor = self._descriptor
        obj._object_is_dict = self._object_is_dict
        return obj

    def _validate(self):
        for idx, obj in enumerate(self._objects):

            if isinstance(obj, dict):
                obj = SimpleNamespace(**obj)
                self._object_is_dict = True
                self._objects[idx] = obj

    def __len__(self):
        return len(self._objects)

    def __repr__(self):
        return f"{self.__class__.__name__}({[self._get_item(i) for i in self._objects]})"

    def __str__(self):
        return str([self._get_item(i) for i in self._objects])

    def __iter__(self):
        if self._object_is_dict:
            return iter(self._get_item(i) for i in self._objects)
        else:
            return self._objects.__iter__()

    def __getitem__(self, key):
        """Provide list-like index access."""
        return self._get_item(self._objects.__getitem__(key))

    def __contains__(self, item):
        if self._objects:
            if type(item) == type(self._get_item(self._objects[0])):
                return self._objects.__contains__(item)
        return False

    def __eq__(self, other):
        return self._objects == other

    def list_attrs(self):
        """Get a tuple of the unique access-attribute values of the constituent objects."""
        return tuple(self._index.keys())

    def _get_item(self, obj):
        if self._object_is_dict:
            return obj.__dict__
        else:
            return obj

    def _get_obj_attr(self, obj, attr):
        """Overriding this function allows control over how the `get` functions behave."""
        return getattr(obj, attr)

    def _get_all_from_objs(self, objs, **kwargs):

        # narrow down according to kwargs:
        specified_objs = []
        for obj in objs:
            skip_obj = False
            for k, v in kwargs.items():
                try:
                    obj_key_val = self._get_obj_attr(obj, k)
                except (AttributeError, KeyError):
                    skip_obj = True
                    break
                if obj_key_val != v:
                    skip_obj = True
                    break
            if skip_obj:
                continue
            else:
                specified_objs.append(obj)

        return [self._get_item(i) for i in specified_objs]

    def get_all(self, **kwargs):
        """Get one or more objects from the object list, by specifying the value of the
        access attribute, and optionally additional keyword-argument attribute values."""

        return self._get_all_from_objs(self._objects, **kwargs)

    def _validate_get(self, result, kwargs):
        if not result:
            available = []
            for obj in self._objects:
                available.append({k: getattr(obj, k) for k in kwargs})
            raise ValueError(
                f"No {self._descriptor} objects with attributes: {kwargs}. Available objects have attributes: {tuple(available)!r}."
            )

        elif len(result) > 1:
            raise ValueError(f"Multiple objects with attributes: {kwargs}.")

        return result[0]

    def get(self, **kwargs):
        """Get a single object from the object list, by specifying the value of the access
        attribute, and optionally additional keyword-argument attribute values."""
        return self._validate_get(self.get_all(**kwargs), kwargs)

    def add_object(self, obj, index=-1):

        if index < 0:
            index += len(self) + 1

        if self._object_is_dict:
            obj = SimpleNamespace(**obj)

        self._objects = self._objects[:index] + [obj] + self._objects[index:]
        self._validate()


class DotAccessObjectList(ObjectList):
    """Provide dot-notation access via an access attribute for the case where the access
    attribute uniquely identifies a single object."""

    def __init__(self, _objects, access_attribute, descriptor=None):
        self._access_attribute = access_attribute
        super().__init__(_objects, descriptor=descriptor)
        self._update_index()

    def _validate(self):
        for idx, obj in enumerate(self._objects):
            if not hasattr(obj, self._access_attribute):
                raise TypeError(
                    f"Object {idx} does not have attribute {self._access_attribute!r}."
                )

        return super()._validate()

    def _update_index(self):
        """For quick look-up by access attribute."""

        _index = {}
        for idx, obj in enumerate(self._objects):
            attr_val = getattr(obj, self._access_attribute)
            try:
                if attr_val in _index:
                    _index[attr_val].append(idx)
                else:
                    _index[attr_val] = [idx]
            except TypeError:
                raise TypeError(
                    f"Access attribute values ({self._access_attribute!r}) must be hashable."
                )
        self._index = _index

    def __getattr__(self, attribute):

        if attribute in self._index:
            idx = self._index[attribute]
            if len(idx) > 1:
                raise ValueError(
                    f"Multiple objects with access attribute: {attribute!r}."
                )
            return self._get_item(self._objects[idx[0]])

        else:
            obj_list_fmt = ", ".join(
                [f'"{getattr(i, self._access_attribute)}"' for i in self._objects]
            )
            msg = f"{self._descriptor.title()} {attribute!r} does not exist. "
            if self._objects:
                msg += f"Available {self._descriptor}s are: {obj_list_fmt}."
            else:
                msg += "The object list is empty."

            raise AttributeError(msg)

    def __dir__(self):
        return super().__dir__() + [
            getattr(i, self._access_attribute) for i in self._objects
        ]

    def get(self, access_attribute_value=None, **kwargs):

        vld_get_kwargs = kwargs
        if access_attribute_value:
            vld_get_kwargs = {self._access_attribute: access_attribute_value, **kwargs}

        return self._validate_get(
            self.get_all(access_attribute_value=access_attribute_value, **kwargs),
            vld_get_kwargs,
        )

    def get_all(self, access_attribute_value=None, **kwargs):

        # use the index to narrow down the search first:
        if access_attribute_value:
            try:
                all_idx = self._index[access_attribute_value]
            except KeyError:
                raise ValueError(
                    f"Value {access_attribute_value!r} does not match the value of any "
                    f"object's attribute {self._access_attribute!r}. Available attribute "
                    f"values are: {self.list_attrs()!r}."
                ) from None
            all_objs = [self._objects[i] for i in all_idx]
        else:
            all_objs = self._objects

        return self._get_all_from_objs(all_objs, **kwargs)

    def add_object(self, obj, index=-1):
        super().add_object(obj, index)
        self._update_index()


class AppDataList(DotAccessObjectList):

    _app_attr = "_app"

    def to_dict(self):
        return {"_objects": super().to_dict()["_objects"]}

    @classmethod
    def from_json_like(cls, json_like, shared_data=None, is_hashed=False):
        """
        Parameters
        ----------
        is_hashed: bool, optional
            If True, accept a dict whose keys are hashes of the dict values.

        """
        if is_hashed:
            json_like = [
                {**obj_js, "_hash_value": hash_val}
                for hash_val, obj_js in json_like.items()
            ]
        return super().from_json_like(
            json_like, shared_data=shared_data or cls._app.app_data
        )


class TaskList(AppDataList):
    """A list-like container for a task-like list with dot-notation access by task
    unique-name."""

    _child_objects = (
        ChildObjectSpec(
            name="_objects",
            class_name="Task",
            is_multiple=True,
            is_single_attribute=True,
        ),
    )

    def __init__(self, _objects):
        super().__init__(_objects, access_attribute="unique_name", descriptor="task")


class TaskTemplateList(AppDataList):
    """A list-like container for a task-like list with dot-notation access by task
    unique-name."""

    _child_objects = (
        ChildObjectSpec(
            name="_objects",
            class_name="TaskTemplate",
            is_multiple=True,
            is_single_attribute=True,
        ),
    )

    def __init__(self, _objects):
        super().__init__(_objects, access_attribute="name", descriptor="task template")


class TaskSchemasList(AppDataList):
    """A list-like container for a task schema list with dot-notation access by task
    schema unique-name."""

    _child_objects = (
        ChildObjectSpec(
            name="_objects",
            class_name="TaskSchema",
            is_multiple=True,
            is_single_attribute=True,
        ),
    )

    def __init__(self, _objects):
        super().__init__(_objects, access_attribute="name", descriptor="task schema")


class GroupList(AppDataList):
    """A list-like container for the task schema group list with dot-notation access by
    group name."""

    _child_objects = (
        ChildObjectSpec(
            name="_objects",
            class_name="Group",
            is_multiple=True,
            is_single_attribute=True,
        ),
    )

    def __init__(self, _objects):
        super().__init__(_objects, access_attribute="name", descriptor="group")


class EnvironmentsList(AppDataList):
    """A list-like container for environments with dot-notation access by name."""

    _child_objects = (
        ChildObjectSpec(
            name="_objects",
            class_name="Environment",
            is_multiple=True,
            is_single_attribute=True,
        ),
    )

    def __init__(self, _objects):
        super().__init__(_objects, access_attribute="name", descriptor="environment")

    def _get_obj_attr(self, obj, attr):
        """Overridden to lookup objects via the `specifiers` dict attribute"""
        if attr in ("name", "_hash_value"):
            return getattr(obj, attr)
        else:
            return getattr(obj, "specifiers")[attr]


class ExecutablesList(AppDataList):
    """A list-like container for environment executables with dot-notation access by
    executable label."""

    environment = None
    _child_objects = (
        ChildObjectSpec(
            name="_objects",
            class_name="Executable",
            is_multiple=True,
            is_single_attribute=True,
            parent_ref="_executables_list",
        ),
    )

    def __init__(self, _objects):
        super().__init__(_objects, access_attribute="label", descriptor="executable")
        self._set_parent_refs()


class ParametersList(AppDataList):
    """A list-like container for parameters with dot-notation access by parameter type."""

    _child_objects = (
        ChildObjectSpec(
            name="_objects",
            class_name="Parameter",
            is_multiple=True,
            is_single_attribute=True,
        ),
    )

    def __init__(self, _objects):
        super().__init__(_objects, access_attribute="typ", descriptor="parameter")


class CommandFilesList(AppDataList):
    """A list-like container for command files with dot-notation access by label."""

    _child_objects = (
        ChildObjectSpec(
            name="_objects",
            class_name="FileSpec",
            is_multiple=True,
            is_single_attribute=True,
        ),
    )

    def __init__(self, _objects):
        super().__init__(_objects, access_attribute="label", descriptor="command file")


class WorkflowTaskList(DotAccessObjectList):
    def __init__(self, _objects):
        super().__init__(_objects, access_attribute="unique_name", descriptor="task")


class ResourceList(ObjectList):

    _app_attr = "_app"
    _child_objects = (
        ChildObjectSpec(
            name="_objects",
            class_name="ResourceSpec",
            is_multiple=True,
            is_single_attribute=True,
            dict_key_attr="scope",
            parent_ref="_resource_list",
        ),
    )

    def __init__(self, _objects):
        super().__init__(_objects, descriptor="resource specification")
        self._element_set = None  # assigned by parent ElementSet
        self._set_parent_refs()

    def __deepcopy__(self, memo):
        obj = super().__deepcopy__(memo)
        obj._element_set = self._element_set
        return obj

    @property
    def element_set(self):
        return self._element_set

    def to_json_like(self, dct=None, shared_data=None, exclude=None, path=None):
        """Overridden to write out as a dict keyed by action scope (like as can be
        specified in the input YAML) instead of list."""

        out, shared_data = super().to_json_like(dct, shared_data, exclude, path)
        as_dict = {}
        for res_spec_js in out:
            scope = self._app.ActionScope.from_json_like(res_spec_js.pop("scope"))
            as_dict[scope.to_string()] = res_spec_js
        return as_dict, shared_data


def index(obj_lst, obj):
    for idx, i in enumerate(obj_lst._objects):
        if obj is i:
            return idx
    raise ValueError(f"{obj!r} not in list.")
