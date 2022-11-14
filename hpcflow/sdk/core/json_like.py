import copy
from dataclasses import dataclass
import enum
import hashlib
import json
from typing import Optional, Type

from .utils import classproperty
from .validation import get_schema
from .errors import ToJSONLikeChildReferenceError


PRIMITIVES = (
    int,
    float,
    str,
    type(None),
)


def to_json_like(obj, shared_data=None, parent_refs=None, path=None):

    path = path or []

    if len(path) > 50:
        raise RuntimeError(f"I'm in too deep! Path is: {path}")

    if isinstance(obj, (list, tuple, set)):
        out = []
        for idx, item in enumerate(obj):
            if hasattr(item, "to_json_like"):
                item, shared_data = item.to_json_like(
                    shared_data=shared_data,
                    exclude=list((parent_refs or {}).values()),
                    path=path + [idx],
                )
            else:
                item, shared_data = to_json_like(
                    item, shared_data=shared_data, path=path + [idx]
                )
            out.append(item)
        if isinstance(obj, tuple):
            out = tuple(out)
        elif isinstance(obj, set):
            out = set(out)

    elif isinstance(obj, dict):
        out = {}
        for dct_key, dct_val in obj.items():
            if hasattr(dct_val, "to_json_like"):
                try:
                    dct_val, shared_data = dct_val.to_json_like(
                        shared_data=shared_data,
                        exclude=[(parent_refs or {}).get(dct_key)],
                        path=path + [dct_key],
                    )
                except ToJSONLikeChildReferenceError:
                    continue
            else:
                dct_val, shared_data = to_json_like(
                    dct_val,
                    shared_data=shared_data,
                    parent_refs=parent_refs,
                    path=path + [dct_key],
                )
            out.update({dct_key: dct_val})

    elif isinstance(obj, PRIMITIVES):
        out = obj

    elif isinstance(obj, enum.Enum):
        out = obj.name

    else:
        out, shared_data = obj.to_json_like(shared_data=shared_data, path=path)

    return out, shared_data


@dataclass
class ChildObjectSpec:
    name: str
    class_name: Optional[str] = None
    class_obj: Optional[
        Type
    ] = None  # TODO: no need for class_obj/class_name if shared data?
    json_like_name: Optional[str] = None
    is_multiple: Optional[bool] = False
    dict_key_attr: Optional[str] = None
    dict_val_attr: Optional[str] = None
    parent_ref: Optional[
        str
    ] = None  # TODO: do parent refs make sense when from shared? Prob not.
    is_single_attribute: Optional[
        bool
    ] = False  # if True, obj is not represented as a dict of attr name-values, but just a value.
    is_enum: Optional[
        bool
    ] = False  # if true, we don't invoke to/from_json_like on the data/Enum
    is_dict_values: Optional[
        bool
    ] = False  # if True, the child object is a dict, whose values are of the specified class. The dict structure will remain.
    is_dict_values_ensure_list: Optional[
        bool
    ] = False  # if True, values that are not lists are cast to lists and multiple child objects are instantiated for each dict value

    shared_data_name: Optional[str] = None
    shared_data_primary_key: Optional[str] = None
    # shared_data_secondary_keys: Optional[Tuple[str]] = None # TODO: what's the point?

    def __post_init__(self):

        if self.class_name is not None and self.class_obj is not None:
            raise ValueError(f"Specify at most one of `class_name` and `class_obj`.")

        if self.dict_key_attr:
            if not isinstance(self.dict_key_attr, str):
                raise TypeError(
                    f"`dict_key_attr` must be of type `str`, but has type "
                    f"{type(self.dict_key_attr)} with value {self.dict_key_attr}."
                )  # TODO: test raise
        if self.dict_val_attr:
            if not self.dict_key_attr:
                raise ValueError(
                    f"If `dict_val_attr` is specified, `dict_key_attr` must be specified."
                )  # TODO: test raise
            if not isinstance(self.dict_val_attr, str):
                raise TypeError(
                    f"`dict_val_attr` must be of type `str`, but has type "
                    f"{type(self.dict_val_attr)} with value {self.dict_val_attr}."
                )  # TODO: test raise
        if not self.is_multiple and self.dict_key_attr:
            raise ValueError(
                f"If `dict_key_attr` is specified, `is_multiple` must be set to True."
            )
        if not self.is_multiple and self.is_dict_values:
            raise ValueError(
                f"If `is_dict_values` is specified, `is_multiple` must be set to True."
            )
        if self.is_dict_values_ensure_list and not self.is_dict_values:
            raise ValueError(
                "If `is_dict_values_ensure_list` is specified, `is_dict_values` must be "
                "set to True."
            )
        if self.parent_ref:
            if not isinstance(self.parent_ref, str):
                raise TypeError(
                    f"`parent_ref` must be of type `str`, but has type "
                    f"{type(self.parent_ref)} with value {self.parent_ref}."
                )  # TODO: test raise

        self.json_like_name = self.json_like_name or self.name


class BaseJSONLike:
    """
    Attributes
    ----------
    _class_namespace : namespace
        Namespace whose attributes include the class definitions that might be
        referenced (and so require instantiation) in child objects.
    _shared_data_namespace : namespace
        Namespace whose attributes include the shared data that might be referenced
        in child objects.
    """

    _child_objects = None
    _validation_schema = None

    __class_namespace = None
    __class_namespace_is_dict = False

    @classmethod
    def _set_class_namespace(cls, value, is_dict=False):
        cls.__class_namespace = value
        cls.__class_namespace_is_dict = is_dict

    @classproperty
    def _class_namespace(cls):
        if not cls.__class_namespace:
            raise ValueError(f"`{cls.__name__}` `class_namespace` must be set!")
        return cls.__class_namespace

    @classmethod
    def _get_child_class(cls, child_obj_spec):
        if child_obj_spec.class_obj:
            return child_obj_spec.class_obj
        elif child_obj_spec.class_name:
            if cls.__class_namespace_is_dict:
                return cls._class_namespace[child_obj_spec.class_name]
            else:
                return getattr(cls._class_namespace, child_obj_spec.class_name)
        else:
            return None

    @classmethod
    def from_json_like(cls, json_like, shared_data=None):
        """
        Parameters
        ----------
        json_like
        shared_data : dict of (str: ObjectList)

        """

        def _from_json_like_item(child_obj_spec, json_like_i):

            if not (
                child_obj_spec.class_name
                or child_obj_spec.class_obj
                or child_obj_spec.is_multiple
                or child_obj_spec.shared_data_name
            ):
                # Nothing to process:
                return json_like_i

            multi_chd_objs = []

            # (if is_dict_values) indices into multi_chd_objs that enable reconstruction
            # of the source dict:
            is_dict_values_idx = {}

            if child_obj_spec.is_multiple:
                if type(json_like_i) == dict:
                    if child_obj_spec.is_dict_values:
                        # keep as a dict
                        for k, v in json_like_i.items():

                            if child_obj_spec.is_dict_values_ensure_list:
                                if not isinstance(v, list):
                                    v = [v]
                            else:
                                v = [v]

                            for i in v:
                                new_multi_idx = len(multi_chd_objs)
                                if k not in is_dict_values_idx:
                                    is_dict_values_idx[k] = []
                                is_dict_values_idx[k].append(new_multi_idx)
                                multi_chd_objs.append(i)

                    else:
                        # want to cast to a list
                        if not child_obj_spec.dict_key_attr:
                            raise ValueError(
                                f"{cls.__name__!r}: must specify a `dict_key_attr` for child "
                                f"object spec {child_obj_spec.name!r}."
                            )

                        for k, v in json_like_i.items():
                            all_attrs = {child_obj_spec.dict_key_attr: k}
                            if child_obj_spec.dict_val_attr:
                                all_attrs[child_obj_spec.dict_val_attr] = v
                            else:
                                if not isinstance(v, dict):
                                    raise TypeError(
                                        f"Value for key {k!r} must be a dict representing "
                                        f"attributes of the {child_obj_spec.name!r} child "
                                        f"object (parent: {cls.__name__!r}). If it instead "
                                        f"represents a single attribute, set the "
                                        f"`dict_val_attr` of the child object spec."
                                    )
                                all_attrs.update(v)
                            multi_chd_objs.append(all_attrs)

                elif type(json_like_i) == list:
                    multi_chd_objs = json_like_i

                else:
                    raise TypeError(
                        f"Child object {child_obj_spec.name} of {cls.__name__!r} must a "
                        f"list or dict."
                    )
            else:
                multi_chd_objs = [json_like_i]

            out = []
            if chd.shared_data_name:
                for i in multi_chd_objs:

                    if i is None:
                        out.append(i)
                        continue

                    if isinstance(i, str):
                        if i.startswith("hash:"):
                            sd_lookup_kwargs = {"_hash_value": i.split("hash:")[1]}
                        else:
                            sd_lookup_kwargs = {chd.shared_data_primary_key: i}
                    elif isinstance(i, dict):
                        sd_lookup_kwargs = i
                    else:
                        raise TypeError(
                            "Shared data reference must be a str or a dict."
                        )  # TODO: test raise
                    chd_obj = shared_data[chd.shared_data_name].get(**sd_lookup_kwargs)
                    out.append(chd_obj)
            else:
                chd_cls = cls._get_child_class(child_obj_spec)
                if child_obj_spec.is_enum:
                    out = []
                    for i in multi_chd_objs:
                        if i is not None:
                            try:
                                i = getattr(chd_cls, i.upper())
                            except AttributeError:
                                raise ValueError(
                                    f"Enumeration {chd_cls!r} has no name {i!r}. Available"
                                    f" names are: {chd_cls._member_names_!r}."
                                )
                        out.append(i)
                else:
                    out = []
                    for i in multi_chd_objs:
                        if i is not None:
                            i = chd_cls.from_json_like(i, shared_data)
                        out.append(i)

            if child_obj_spec.is_dict_values:
                out_dict = {}
                for k, v in is_dict_values_idx.items():
                    out_dict[k] = [out[i] for i in v]
                    if not child_obj_spec.is_dict_values_ensure_list:
                        out_dict[k] = out_dict[k][0]
                out = out_dict

            elif not child_obj_spec.is_multiple:
                out = out[0]

            return out

        if cls._validation_schema:
            validation_schema = get_schema(cls._validation_schema)
            validated = validation_schema.validate(json_like)
            if not validated.is_valid:
                raise ValueError(validated.get_failures_string())

        if json_like is None:
            # e.g. optional attributes # TODO: is this still needed?
            return None

        shared_data = shared_data or {}
        json_like = copy.deepcopy(json_like)

        for chd in cls._child_objects or []:

            if chd.is_single_attribute:
                if len(cls._child_objects) > 1:
                    raise TypeError(
                        f"If ChildObjectSpec has `is_single_attribute=True`, only one "
                        f"ChildObjectSpec may be specified on the class. Specified child "
                        f"objects specs are: {cls._child_objects!r}."
                    )
                json_like = {chd.name: json_like}

            if chd.json_like_name in json_like:
                json_like_i = json_like.pop(chd.json_like_name)
                json_like[chd.name] = _from_json_like_item(chd, json_like_i)

        need_hash = False
        if hasattr(cls, "_hash_value"):
            if "_hash_value" not in json_like:
                need_hash = True

        try:
            if hasattr(cls, "_json_like_constructor"):
                obj = cls._json_like_constructor(json_like)
            else:
                obj = cls(**json_like)
        except TypeError as err:
            raise TypeError(
                f"Failed initialisation of class {cls.__name__!r}. Check the signature. "
                f"Caught TypeError: {err}"
            )

        if need_hash:
            re_json_like = obj.to_json_like()[0]
            re_json_like.pop("_hash_value", None)
            hash_val = cls._get_hash(re_json_like)
            obj._hash_value = hash_val

        return obj

    def _set_parent_refs(self):
        """Assign references to self on child objects that declare a parent ref
        attribute."""

        for chd in self._child_objects:
            if chd.parent_ref:
                if chd.is_multiple:
                    for i in getattr(self, chd.name):
                        setattr(i, chd.parent_ref, self)
                else:
                    setattr(getattr(self, chd.name), chd.parent_ref, self)

    @staticmethod
    def _get_hash(json_like):
        json_str_i = json.dumps(json_like, sort_keys=True)
        return hashlib.md5(json_str_i.encode("utf-8")).hexdigest()

    def to_dict(self):
        if hasattr(self, "__dict__"):
            return dict(self.__dict__)
        elif hasattr(self, "__slots__"):
            return {k: getattr(self, k) for k in self.__slots__}

    def to_json_like(self, dct=None, shared_data=None, exclude=None, path=None):

        if dct is None:
            dct = {k: v for k, v in self.to_dict().items() if k not in (exclude or [])}

        parent_refs = {}
        for chd in self._child_objects or []:

            if chd.is_single_attribute:
                if len(self._child_objects) > 1:
                    raise TypeError(
                        f"If ChildObjectSpec has `is_single_attribute=True`, only one "
                        f"ChildObjectSpec may be specified on the class."
                    )
                dct = dct[chd.json_like_name]

            if chd.parent_ref:
                parent_refs.update({chd.name: chd.parent_ref})

        json_like, shared_data = to_json_like(
            dct, shared_data=shared_data, parent_refs=parent_refs, path=path
        )
        shared_data = shared_data or {}

        for chd in self._child_objects or []:

            if chd.name in json_like:
                json_like[chd.json_like_name] = json_like.pop(chd.name)

            if chd.shared_data_name:
                if chd.shared_data_name not in shared_data:
                    shared_data[chd.shared_data_name] = {}

                chd_obj_js = json_like.pop(chd.json_like_name)

                if not chd.is_multiple:
                    chd_obj_js = [chd_obj_js]

                shared_keys = []
                for i in chd_obj_js:

                    i.pop("_hash_value", None)
                    hash_i = self._get_hash(i)
                    shared_keys.append(f"hash:{hash_i}")

                    if hash_i not in shared_data[chd.shared_data_name]:
                        shared_data[chd.shared_data_name].update({hash_i: i})

                if not chd.is_multiple:
                    shared_keys = shared_keys[0]

                json_like[chd.json_like_name] = shared_keys

        return json_like, shared_data


class JSONLike(BaseJSONLike):
    """BaseJSONLike, where the class namespace is the App instance."""

    _app_attr = "app"  # for some classes we change this to "_app"

    @classproperty
    def _class_namespace(cls):
        return getattr(cls, cls._app_attr)

    def to_dict(self):

        out = super().to_dict()

        # remove parent references:
        app = self._class_namespace
        for sub_cls in app._core_classes:
            if hasattr(sub_cls, "_child_objects"):
                for chd in sub_cls._child_objects or []:
                    if chd.parent_ref:
                        if (
                            self.__class__.__name__ == chd.class_name
                            or self.__class__ is chd.class_obj
                        ):
                            out.pop(chd.parent_ref, None)
        return out
