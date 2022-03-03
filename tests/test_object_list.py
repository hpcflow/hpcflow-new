from dataclasses import dataclass

import pytest

from hpcflow.object_list import DotAccessObjectList


@dataclass
class MyObj:
    name: str
    data: int


@pytest.fixture
def simple_object_list():

    my_objs = [MyObj(name="A", data=1), MyObj(name="B", data=2)]
    obj_list = DotAccessObjectList(
        *my_objs, access_attribute="name", descriptor="my_object"
    )
    out = {
        "objects": my_objs,
        "object_list": obj_list,
    }
    return out


def test_get_item(simple_object_list):

    objects = simple_object_list["objects"]
    obj_list = simple_object_list["object_list"]

    assert obj_list[0] == objects[0] and obj_list[1] == objects[1]


def test_get_dot_notation(simple_object_list):

    objects = simple_object_list["objects"]
    obj_list = simple_object_list["object_list"]

    assert obj_list.A == objects[0] and obj_list.B == objects[1]


def test_add_obj_to_end(simple_object_list):
    obj_list = simple_object_list["object_list"]
    new_obj = MyObj("C", 3)
    obj_list.add_object(new_obj)
    assert obj_list[-1] == new_obj


def test_add_obj_to_start(simple_object_list):
    obj_list = simple_object_list["object_list"]
    new_obj = MyObj("C", 3)
    obj_list.add_object(new_obj, 0)
    assert obj_list[0] == new_obj


def test_add_obj_to_middle(simple_object_list):
    obj_list = simple_object_list["object_list"]
    new_obj = MyObj("C", 3)
    obj_list.add_object(new_obj, 1)
    assert obj_list[1] == new_obj
