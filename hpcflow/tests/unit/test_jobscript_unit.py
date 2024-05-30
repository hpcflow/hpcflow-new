from hpcflow.app import app as hf
from hpcflow.sdk.core.test_utils import make_schemas, make_workflow
from hpcflow.sdk.submission.jobscript import is_jobscript_array, resolve_jobscript_blocks

import pytest


def test_resolve_jobscript_blocks():
    # separate jobscripts due to `is_array`:
    jobscripts = {
        0: {"is_array": True, "resource_hash": 0, "dependencies": {}},
        1: {"is_array": True, "resource_hash": 0, "dependencies": {0: "DEP_DATA"}},
    }
    assert resolve_jobscript_blocks(jobscripts) == [
        {"resources": None, "is_array": True, "blocks": [{"dependencies": {}}]},
        {
            "resources": None,
            "is_array": True,
            "blocks": [{"dependencies": {(0, 0): "DEP_DATA"}}],
        },
    ]

    # separate jobscripts due to different `resource_hash`:
    jobscripts = {
        0: {"is_array": False, "resource_hash": 0, "dependencies": {}},
        1: {"is_array": False, "resource_hash": 1, "dependencies": {0: "DEP_DATA"}},
    }
    assert resolve_jobscript_blocks(jobscripts) == [
        {"resources": None, "is_array": False, "blocks": [{"dependencies": {}}]},
        {
            "resources": None,
            "is_array": False,
            "blocks": [{"dependencies": {(0, 0): "DEP_DATA"}}],
        },
    ]

    # separate jobscripts due to `is_array`:
    jobscripts = {
        0: {"is_array": False, "resource_hash": 0, "dependencies": {}},
        1: {"is_array": True, "resource_hash": 0, "dependencies": {0: "DEP_DATA"}},
    }
    assert resolve_jobscript_blocks(jobscripts) == [
        {"resources": None, "is_array": False, "blocks": [{"dependencies": {}}]},
        {
            "resources": None,
            "is_array": True,
            "blocks": [{"dependencies": {(0, 0): "DEP_DATA"}}],
        },
    ]

    # separate jobscripts due to `is_array`:
    jobscripts = {
        0: {"is_array": True, "resource_hash": 0, "dependencies": {}},
        1: {"is_array": False, "resource_hash": 0, "dependencies": {0: "DEP_DATA"}},
    }
    assert resolve_jobscript_blocks(jobscripts) == [
        {"resources": None, "is_array": True, "blocks": [{"dependencies": {}}]},
        {
            "resources": None,
            "is_array": False,
            "blocks": [{"dependencies": {(0, 0): "DEP_DATA"}}],
        },
    ]

    # combined jobscript due to same resource_hash, not is_array, and dependencies:
    jobscripts = {
        0: {"is_array": False, "resource_hash": 0, "dependencies": {}},
        1: {"is_array": False, "resource_hash": 0, "dependencies": {0: "DEP_DATA"}},
        2: {"is_array": False, "resource_hash": 0, "dependencies": {1: "DEP_DATA"}},
    }
    assert resolve_jobscript_blocks(jobscripts) == [
        {
            "resources": None,
            "is_array": False,
            "blocks": [
                {"dependencies": {}},
                {"dependencies": {(0, 0): "DEP_DATA"}},
                {"dependencies": {(0, 1): "DEP_DATA"}},
            ],
        }
    ]

    # combined jobscript due to same resource_hash, not is_array, and dependencies:
    # (checking non-consecutive jobscript index `3` is inconsequential)
    jobscripts = {
        0: {"is_array": False, "resource_hash": 0, "dependencies": {}},
        1: {"is_array": False, "resource_hash": 0, "dependencies": {0: "DEP_DATA"}},
        3: {"is_array": False, "resource_hash": 0, "dependencies": {1: "DEP_DATA"}},
    }
    assert resolve_jobscript_blocks(jobscripts) == [
        {
            "resources": None,
            "is_array": False,
            "blocks": [
                {"dependencies": {}},
                {"dependencies": {(0, 0): "DEP_DATA"}},
                {"dependencies": {(0, 1): "DEP_DATA"}},
            ],
        }
    ]

    # jobscript 0 and 1 combined, not 2 due to independence:
    jobscripts = {
        0: {"is_array": False, "resource_hash": 0, "dependencies": {}},
        1: {"is_array": False, "resource_hash": 0, "dependencies": {0: "DEP_DATA"}},
        2: {"is_array": False, "resource_hash": 0, "dependencies": {}},
    }
    assert resolve_jobscript_blocks(jobscripts) == [
        {
            "resources": None,
            "is_array": False,
            "blocks": [{"dependencies": {}}, {"dependencies": {(0, 0): "DEP_DATA"}}],
        },
        {"resources": None, "is_array": False, "blocks": [{"dependencies": {}}]},
    ]

    # separate jobscripts 0,1 due to independence, separate jobscript 2 due to dependence
    # that spans multiple upstream jobscripts:
    jobscripts = {
        0: {"is_array": False, "resource_hash": 0, "dependencies": {}},
        1: {"is_array": False, "resource_hash": 0, "dependencies": {}},
        2: {
            "is_array": False,
            "resource_hash": 0,
            "dependencies": {0: "DEP_DATA", 1: "DEP_DATA"},
        },
    }
    assert resolve_jobscript_blocks(jobscripts) == [
        {"resources": None, "is_array": False, "blocks": [{"dependencies": {}}]},
        {"resources": None, "is_array": False, "blocks": [{"dependencies": {}}]},
        {
            "resources": None,
            "is_array": False,
            "blocks": [{"dependencies": {(0, 0): "DEP_DATA", (1, 0): "DEP_DATA"}}],
        },
    ]

    # combine jobscripts due to dependence
    jobscripts = {
        0: {"is_array": False, "resource_hash": 0, "dependencies": {}},
        1: {"is_array": False, "resource_hash": 0, "dependencies": {0: "DEP_DATA"}},
        2: {
            "is_array": False,
            "resource_hash": 0,
            "dependencies": {0: "DEP_DATA", 1: "DEP_DATA"},
        },
    }
    assert resolve_jobscript_blocks(jobscripts) == [
        {
            "resources": None,
            "is_array": False,
            "blocks": [
                {"dependencies": {}},
                {"dependencies": {(0, 0): "DEP_DATA"}},
                {"dependencies": {(0, 0): "DEP_DATA", (0, 1): "DEP_DATA"}},
            ],
        }
    ]


def test_is_job_array_raises_on_bad_scheduler():
    resources = hf.ElementResources(use_job_array=True)
    resources.set_defaults()
    with pytest.raises(ValueError):
        is_jobscript_array(resources=resources, num_elements=2, store=None)


def test_force_array(null_config, tmp_path):
    wk = make_workflow(
        [[{"p1": None}, ("p2",), "t1"]],
        path=tmp_path,
        local_sequences={0: [("inputs.p1", 2, 0)]},
        name="w1",
        overwrite=False,
    )
    sub = wk.add_submission(force_array=True)
    assert len(sub.jobscripts) == 1
    assert sub.jobscripts[0].is_array


def test_merge_jobscript_multi_dependence(null_config, tmp_path):
    s1, s2, s3 = make_schemas(
        [
            [{}, ("p1", "p2"), "t1"],
            [
                {
                    "p1": None,
                },
                ("p3",),
                "t2",
            ],
            [{"p1": None, "p3": None}, tuple(), "t3"],
        ]
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_merge_js",
        workflow_name="test_merge_js",
        overwrite=True,
        tasks=[
            hf.Task(schema=s1, repeats=2),
            hf.Task(schema=s2),
            hf.Task(schema=s3),
        ],
    )
    sub = wk.add_submission()
    assert len(sub.jobscripts) == 1
    assert len(sub.jobscripts[0].blocks) == 1


def test_merge_jobscript_multi_dependence_non_array_source(null_config, tmp_path):
    # the second two jobscripts should merge
    s1, s2, s3 = make_schemas(
        [
            [{}, ("p1", "p2"), "t1"],
            [
                {
                    "p1": None,
                },
                ("p3",),
                "t2",
            ],
            [{"p1": None, "p3": None}, tuple(), "t3"],
        ]
    )
    wk = hf.Workflow.from_template_data(
        template_name="wk_test_merge",
        tasks=[
            hf.Task(schema=s1),
            hf.Task(schema=s2, repeats=2),
            hf.Task(schema=s3),
        ],
    )
    sub = wk.add_submission(force_array=True)

    assert len(sub.jobscripts) == 2
    assert len(sub.jobscripts[0].blocks) == 1
    assert len(sub.jobscripts[1].blocks) == 1
