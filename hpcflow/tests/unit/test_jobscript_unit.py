from hpcflow.sdk.submission.jobscript import resolve_jobscript_blocks


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
