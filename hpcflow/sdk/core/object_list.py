from __future__ import annotations
import copy
from types import SimpleNamespace
from typing import Generic, TypeVar, cast, overload, TYPE_CHECKING

from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike, JSONable, JSONed
from hpcflow.sdk.core.task import ElementSet
from hpcflow.sdk.core.workflow import WorkflowTemplate
if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping, Sequence
    from typing import ClassVar, Self, Literal
    from zarr import Group  # type: ignore
    from ..app import BaseApp
    from .actions import ActionScope
    from .command_files import FileSpec
    from .environment import Environment, Executable
    from .loop import WorkflowLoop
    from .parameters import Parameter, ResourceSpec
    from .task import Task, TaskTemplate, TaskSchema, WorkflowTask

T = TypeVar("T")


class ObjectListMultipleMatchError(ValueError):
    pass


class ObjectList(JSONLike, Generic[T]):
    """A list-like class that provides item access via a `get` method according to
    attributes or dict-keys.

    """

    def __init__(self, objects: Iterable[T], descriptor: str | None = None):
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
        self._object_is_dict: bool = False

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
        return repr(self._objects)

    def __str__(self):
        return str([self._get_item(i) for i in self._objects])

    def __iter__(self) -> Iterator[T]:
        if self._object_is_dict:
            return iter(self._get_item(i) for i in self._objects)
        else:
            return self._objects.__iter__()

    @overload
    def __getitem__(self, key: int) -> T: ...

    @overload
    def __getitem__(self, key: slice) -> list[T]: ...

    def __getitem__(self, key) -> T | list[T]:
        """Provide list-like index access."""
        result = self._objects.__getitem__(key)
        return list(map(self._get_item, result)) if isinstance(key, slice) else self._get_item(result)

    def __contains__(self, item: T) -> bool:
        if self._objects:
            if type(item) == type(self._get_item(self._objects[0])):
                return self._objects.__contains__(item)
        return False

    def __eq__(self, other) -> bool:
        return self._objects == other

    def list_attrs(self):
        """Get a tuple of the unique access-attribute values of the constituent objects."""
        return tuple(self._index.keys())

    def _get_item(self, obj: T):
        if self._object_is_dict:
            return obj.__dict__
        else:
            return obj

    def _get_obj_attr(self, obj: T, attr: str):
        """Overriding this function allows control over how the `get` functions behave."""
        return getattr(obj, attr)

    def _get_all_from_objs(self, objs: Iterable[T], **kwargs):
        # narrow down according to kwargs:
        specified_objs: list[T] = []
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

    def _validate_get(self, result: Sequence[T], kwargs):
        if not result:
            available = []
            for obj in self._objects:
                attr_vals = {}
                for k in kwargs:
                    try:
                        attr_vals[k] = self._get_obj_attr(obj, k)
                    except (AttributeError, KeyError):
                        continue
                available.append(attr_vals)
            raise ValueError(
                f"No {self._descriptor} objects with attributes: {kwargs}. Available "
                f"objects have attributes: {tuple(available)!r}."
            )

        elif len(result) > 1:
            raise ObjectListMultipleMatchError(
                f"Multiple objects with attributes: {kwargs}."
            )

        return result[0]

    def get(self, **kwargs):
        """Get a single object from the object list, by specifying the value of the access
        attribute, and optionally additional keyword-argument attribute values."""
        return self._validate_get(self.get_all(**kwargs), kwargs)

    @overload
    def add_object(
        self, obj: T, index: int = -1, *,
        skip_duplicates: Literal[False] = False
    ) -> int: ...

    @overload
    def add_object(
        self, obj: T, index: int = -1, *,
        skip_duplicates: Literal[True]
    ) -> int | None: ...

    def add_object(
        self, obj: T, index: int = -1, *,
        skip_duplicates: bool = False
    ) -> None | int:
        if skip_duplicates and obj in self:
            return None

        if index < 0:
            index += len(self) + 1

        if self._object_is_dict:
            obj = cast(T, SimpleNamespace(**cast(dict, obj)))

        self._objects = self._objects[:index] + [obj] + self._objects[index:]
        self._validate()
        return index


class DotAccessObjectList(ObjectList[T], Generic[T]):
    """Provide dot-notation access via an access attribute for the case where the access
    attribute uniquely identifies a single object."""

    # access attributes must not be named after any "public" methods, to avoid confusion!
    _pub_methods = ("get", "get_all", "add_object", "add_objects")

    def __init__(self, _objects: Iterable[T], access_attribute: str, descriptor: str | None = None):
        self._access_attribute = access_attribute
        super().__init__(_objects, descriptor=descriptor)
        self._update_index()

    def _validate(self):
        for idx, obj in enumerate(self._objects):
            if not hasattr(obj, self._access_attribute):
                raise TypeError(
                    f"Object {idx} does not have attribute {self._access_attribute!r}."
                )
            value = getattr(obj, self._access_attribute)
            if value in self._pub_methods:
                raise ValueError(
                    f"Access attribute {self._access_attribute!r} for object index {idx} "
                    f"cannot be the same as any of the methods of "
                    f"{self.__class__.__name__!r}, which are: {self._pub_methods!r}."
                )

        return super()._validate()

    def _update_index(self) -> None:
        """For quick look-up by access attribute."""

        _index: dict[str, list[int]] = {}
        for idx, obj in enumerate(self._objects):
            attr_val: str = getattr(obj, self._access_attribute)
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

    def __getattr__(self, attribute: str):
        if attribute in self._index:
            idx = self._index[attribute]
            if len(idx) > 1:
                raise ValueError(
                    f"Multiple objects with access attribute: {attribute!r}."
                )
            return self._get_item(self._objects[idx[0]])

        elif not attribute.startswith("__"):
            obj_list_fmt = ", ".join(
                [f'"{getattr(i, self._access_attribute)}"' for i in self._objects]
            )
            msg = f"{self._descriptor.title()} {attribute!r} does not exist. "
            if self._objects:
                msg += f"Available {self._descriptor}s are: {obj_list_fmt}."
            else:
                msg += "The object list is empty."

            raise AttributeError(msg)
        else:
            raise AttributeError

    def __dir__(self):
        return super().__dir__() + [
            getattr(i, self._access_attribute) for i in self._objects
        ]

    def get(self, access_attribute_value: str | None = None, **kwargs):
        vld_get_kwargs = kwargs
        if access_attribute_value is not None:
            vld_get_kwargs = {self._access_attribute: access_attribute_value, **kwargs}

        return self._validate_get(
            self.get_all(access_attribute_value=access_attribute_value, **kwargs),
            vld_get_kwargs,
        )

    def get_all(self, access_attribute_value: str | None = None, **kwargs):
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

    @overload
    def add_object(
        self, obj: T, index: int = -1, *,
        skip_duplicates: Literal[False] = False
    ) -> int: ...

    @overload
    def add_object(
        self, obj: T, index: int = -1, *,
        skip_duplicates: Literal[True]
    ) -> int | None: ...

    def add_object(
        self, obj: T, index: int = -1, *,
        skip_duplicates: bool = False
    ) -> int | None:
        if skip_duplicates:
            new_index = super().add_object(obj, index, skip_duplicates=True)
        else:
            new_index = super().add_object(obj, index)
        self._update_index()
        return new_index

    def add_objects(self, objs: Iterable[T], index: int = -1, *,
                    skip_duplicates: bool = False):
        if skip_duplicates:
            for obj in objs:
                index_ = self.add_object(obj, index, skip_duplicates=True)
                if index_ is not None:
                    index = index_ + 1
        else:
            for obj in objs:
                index = self.add_object(obj, index) + 1
        return index


class AppDataList(DotAccessObjectList[T], Generic[T]):
    _app: ClassVar[BaseApp]
    _app_attr = "_app"

    def to_dict(self):
        return {"_objects": super().to_dict()["_objects"]}

    @classmethod
    def _get_default_shared_data(cls) -> Mapping[str, ObjectList[JSONable]]:
        return cls._app._shared_data

    @overload
    @classmethod
    def from_json_like(cls, json_like: str,
                       shared_data: Mapping[str, ObjectList[JSONable]] | None = None,
                       is_hashed: bool = False) -> Self | None: ...

    @overload
    @classmethod
    def from_json_like(cls, json_like: Mapping[str, JSONed] | Sequence[Mapping[str, JSONed]],
                       shared_data: Mapping[str, ObjectList[JSONable]] | None = None,
                       is_hashed: bool = False) -> Self: ...

    @overload
    @classmethod
    def from_json_like(cls, json_like: None,
                       shared_data: Mapping[str, ObjectList[JSONable]] | None = None,
                       is_hashed: bool = False) -> None: ...

    @classmethod
    def from_json_like(cls, json_like: str | Mapping[str, JSONed] | Sequence[Mapping[str, JSONed]] | None,
                       shared_data: Mapping[str, ObjectList[JSONable]] | None = None,
                       is_hashed: bool = False) -> Self | None:
        """
        Parameters
        ----------
        is_hashed
            If True, accept a dict whose keys are hashes of the dict values.

        """
        if is_hashed:
            assert isinstance(json_like, Mapping)
            return super().from_json_like([
                {**cast(Mapping, obj_js), "_hash_value": hash_val}
                for hash_val, obj_js in json_like.items()
            ], shared_data=shared_data)
        else:
            return super().from_json_like(json_like, shared_data=shared_data)    

    def _remove_object(self, index: int):
        self._objects.pop(index)
        self._update_index()


class TaskList(AppDataList[Task]):
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

    def __init__(self, _objects: Iterable[Task]):
        super().__init__(_objects, access_attribute="unique_name", descriptor="task")


class TaskTemplateList(AppDataList[TaskTemplate]):
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

    def __init__(self, _objects: Iterable[TaskTemplate]):
        super().__init__(_objects, access_attribute="name", descriptor="task template")


class TaskSchemasList(AppDataList[TaskSchema]):
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

    def __init__(self, _objects: Iterable[TaskSchema]):
        super().__init__(_objects, access_attribute="name", descriptor="task schema")


class GroupList(AppDataList[Group]):
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

    def __init__(self, _objects: Iterable[Group]):
        super().__init__(_objects, access_attribute="name", descriptor="group")


class EnvironmentsList(AppDataList[Environment]):
    """A list-like container for environments with dot-notation access by name."""

    _child_objects = (
        ChildObjectSpec(
            name="_objects",
            class_name="Environment",
            is_multiple=True,
            is_single_attribute=True,
        ),
    )

    def __init__(self, _objects: Iterable[Environment]):
        super().__init__(_objects, access_attribute="name", descriptor="environment")

    def _get_obj_attr(self, obj: Environment, attr: str):
        """Overridden to lookup objects via the `specifiers` dict attribute"""
        if attr in ("name", "_hash_value"):
            return getattr(obj, attr)
        else:
            return getattr(obj, "specifiers")[attr]


class ExecutablesList(AppDataList[Executable]):
    """A list-like container for environment executables with dot-notation access by
    executable label."""

    environment: Environment | None = None
    _child_objects = (
        ChildObjectSpec(
            name="_objects",
            class_name="Executable",
            is_multiple=True,
            is_single_attribute=True,
            parent_ref="_executables_list",
        ),
    )

    def __init__(self, _objects: Iterable[Executable]):
        super().__init__(_objects, access_attribute="label", descriptor="executable")
        self._set_parent_refs()

    def __deepcopy__(self, memo):
        obj = super().__deepcopy__(memo)
        obj.environment = self.environment
        return obj


class ParametersList(AppDataList[Parameter]):
    """A list-like container for parameters with dot-notation access by parameter type."""

    _child_objects = (
        ChildObjectSpec(
            name="_objects",
            class_name="Parameter",
            is_multiple=True,
            is_single_attribute=True,
        ),
    )

    def __init__(self, _objects: Iterable[Parameter]):
        super().__init__(_objects, access_attribute="typ", descriptor="parameter")

    def __getattr__(self, attribute) -> Parameter:
        """Overridden to provide a default Parameter object if none exists."""
        try:
            if not attribute.startswith("__"):
                return super().__getattr__(attribute)
        except (AttributeError, ValueError):
            return self._app.Parameter(typ=attribute)
        raise AttributeError

    def get_all(self, access_attribute_value=None, **kwargs):
        """Overridden to provide a default Parameter object if none exists."""
        typ = access_attribute_value if access_attribute_value else kwargs.get("typ")
        try:
            all_out = super().get_all(access_attribute_value, **kwargs)
        except ValueError:
            return [self._app.Parameter(typ=typ)]
        else:
            # `get_all` will not raise `ValueError` if `access_attribute_value` is
            # None and the parameter `typ` is specified in `kwargs` instead:
            return all_out or [self._app.Parameter(typ=typ)]


class CommandFilesList(AppDataList[FileSpec]):
    """A list-like container for command files with dot-notation access by label."""

    _child_objects = (
        ChildObjectSpec(
            name="_objects",
            class_name="FileSpec",
            is_multiple=True,
            is_single_attribute=True,
        ),
    )

    def __init__(self, _objects: Iterable[FileSpec]):
        super().__init__(_objects, access_attribute="label", descriptor="command file")


class WorkflowTaskList(DotAccessObjectList[WorkflowTask]):
    def __init__(self, _objects: Iterable[WorkflowTask]):
        super().__init__(_objects, access_attribute="unique_name", descriptor="task")

    def _reindex(self):
        """Re-assign the WorkflowTask index attributes so they match their order."""
        for idx, i in enumerate(self._objects):
            i._index = idx
        self._update_index()

    def add_object(self, obj: WorkflowTask, index: int = -1, skip_duplicates = False) -> int:
        index = super().add_object(obj, index)
        self._reindex()
        return index

    def _remove_object(self, index: int):
        self._objects.pop(index)
        self._reindex()


class WorkflowLoopList(DotAccessObjectList[WorkflowLoop]):
    def __init__(self, _objects):
        super().__init__(_objects, access_attribute="name", descriptor="loop")

    def _remove_object(self, index: int):
        self._objects.pop(index)


class ResourceList(ObjectList[ResourceSpec]):
    _app: ClassVar[BaseApp]
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

    def __init__(self, _objects: Iterable[ResourceSpec]):
        super().__init__(_objects, descriptor="resource specification")
        self._element_set: ElementSet | None = None  # assigned by parent ElementSet
        self._workflow_template: WorkflowTemplate | None = None  # assigned by parent WorkflowTemplate

        # check distinct scopes for each item:
        scopes = [i.to_string() for i in self.get_scopes()]
        if len(set(scopes)) < len(scopes):
            raise ValueError(
                "Multiple `ResourceSpec` objects have the same scope. The scopes are "
                f"{scopes!r}."
            )

        self._set_parent_refs()

    def __deepcopy__(self, memo):
        obj = super().__deepcopy__(memo)
        obj._element_set = self._element_set
        obj._workflow_template = self._workflow_template
        return obj

    @property
    def element_set(self) -> ElementSet | None:
        return self._element_set

    @property
    def workflow_template(self) -> WorkflowTemplate | None:
        return self._workflow_template

    def to_json_like(self, dct=None, shared_data=None, exclude=None, path=None):
        """Overridden to write out as a dict keyed by action scope (like as can be
        specified in the input YAML) instead of list."""

        out, shared_data = super().to_json_like(dct, shared_data, exclude, path)
        as_dict = {}
        for res_spec_js in out:
            scope = self._app.ActionScope.from_json_like(res_spec_js.pop("scope"))
            as_dict[scope.to_string()] = res_spec_js
        return as_dict, shared_data

    @classmethod
    def normalise(cls, resources: ResourceSpec | None | dict | list) -> Self:
        """Generate from resource-specs specified in potentially several ways."""

        def _ensure_non_persistent(resource_spec):
            # for any resources that are persistent, if they have a
            # `_resource_list` attribute, this means they are sourced from some
            # other persistent workflow, rather than, say, a workflow being
            # loaded right now, so make a non-persistent copy:
            if res_i._value_group_idx is not None and res_i._resource_list is not None:
                return resource_spec.copy_non_persistent()
            return resource_spec

        app = cls._app
        if not resources:
            return cls([app.ResourceSpec()])
        elif isinstance(resources, dict):
            return cls.from_json_like(resources)
        elif isinstance(resources, list):
            for idx, res_i in enumerate(resources):
                if isinstance(res_i, dict):
                    resources[idx] = app.ResourceSpec.from_json_like(res_i)
                else:
                    resources[idx] = _ensure_non_persistent(resources[idx])
            return cls(resources)
        else:
            return cls([resources])

    def get_scopes(self) -> tuple[ActionScope, ...]:
        return tuple(i.scope for i in self._objects if i.scope is not None)

    def merge_other(self, other: ResourceList):
        """Merge lower-precedence other resource list into this resource list."""
        for scope_i in other.get_scopes():
            try:
                self_scoped = self.get(scope=scope_i)
            except ValueError:
                in_self = False
            else:
                in_self = True

            other_scoped = other.get(scope=scope_i)
            if in_self:
                for k, v in other_scoped._get_members().items():
                    if getattr(self_scoped, k) is None:
                        setattr(self_scoped, f"_{k}", copy.deepcopy(v))
            else:
                self.add_object(copy.deepcopy(other_scoped))


def index(obj_lst: ObjectList[T], obj: T) -> int:
    for idx, i in enumerate(obj_lst._objects):
        if obj is i:
            return idx
    raise ValueError(f"{obj!r} not in list.")
