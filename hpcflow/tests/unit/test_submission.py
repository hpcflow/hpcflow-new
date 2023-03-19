from hpcflow.sdk.core.submission import allocate_jobscripts


def test_allocate_jobscripts():
    # x-axis corresponds to elements; y-axis corresponds to actions:
    examples = (
        {
            "resources": [
                [1, 1, 1, 2, -1, 2, 4, -1, 1],
                [1, 3, 1, 2, 2, 2, 4, 4, 1],
                [1, 1, 3, 2, 2, 2, 4, -1, 1],
            ],
            "expected": [
                {"resources": 1, "EARs": {0: [0, 1, 2], 1: [0], 2: [0, 1], 8: [0, 1, 2]}},
                {"resources": 2, "EARs": {3: [0, 1, 2], 4: [1, 2], 5: [0, 1, 2]}},
                {"resources": 4, "EARs": {6: [0, 1, 2], 7: [1]}},
                {"resources": 3, "EARs": {1: [1]}},
                {"resources": 1, "EARs": {1: [2]}},
                {"resources": 3, "EARs": {2: [2]}},
            ],
        },
        {
            "resources": [
                [2, 2, -1],
                [8, 8, 1],
                [4, 4, 1],
            ],
            "expected": [
                {"resources": 2, "EARs": {0: [0], 1: [0]}},
                {"resources": 1, "EARs": {2: [1, 2]}},
                {"resources": 8, "EARs": {0: [1], 1: [1]}},
                {"resources": 4, "EARs": {0: [2], 1: [2]}},
            ],
        },
        {
            "resources": [
                [2, 2, -1],
                [2, 2, 1],
                [4, 4, 1],
            ],
            "expected": [
                {"resources": 2, "EARs": {0: [0, 1], 1: [0, 1]}},
                {"resources": 1, "EARs": {2: [1, 2]}},
                {"resources": 4, "EARs": {0: [2], 1: [2]}},
            ],
        },
        {
            "resources": [
                [2, 1, 2],
                [1, 1, 1],
                [1, 1, 1],
            ],
            "expected": [
                {"resources": 1, "EARs": {1: [0, 1, 2]}},
                {"resources": 2, "EARs": {0: [0], 2: [0]}},
                {"resources": 1, "EARs": {0: [1, 2], 2: [1, 2]}},
            ],
        },
        {
            "resources": [
                [2, -1, 2],
                [1, 1, 1],
                [1, 1, 1],
            ],
            "expected": [
                {"resources": 2, "EARs": {0: [0], 2: [0]}},
                {"resources": 1, "EARs": {0: [1, 2], 1: [1, 2], 2: [1, 2]}},
            ],
        },
        {
            "resources": [
                [1, 1],
                [1, 1],
                [1, 1],
            ],
            "expected": [{"resources": 1, "EARs": {0: [0, 1, 2], 1: [0, 1, 2]}}],
        },
        {
            "resources": [
                [1, 1, 1],
                [1, 1, -1],
                [1, 1, 1],
            ],
            "expected": [
                {"resources": 1, "EARs": {0: [0, 1, 2], 1: [0, 1, 2], 2: [0, 2]}}
            ],
        },
        {
            "resources": [
                [1, 1, -1],
                [1, 1, 1],
                [1, 1, 1],
            ],
            "expected": [
                {"resources": 1, "EARs": {0: [0, 1, 2], 1: [0, 1, 2], 2: [1, 2]}}
            ],
        },
        {
            "resources": [
                [2, 2, -1],
                [4, 4, 1],
                [4, 4, -1],
                [2, 2, 1],
            ],
            "expected": [
                {"resources": 2, "EARs": {0: [0], 1: [0]}},
                {"resources": 1, "EARs": {2: [1, 3]}},
                {"resources": 4, "EARs": {0: [1, 2], 1: [1, 2]}},
                {"resources": 2, "EARs": {0: [3], 1: [3]}},
            ],
        },
        {
            "resources": [
                [2, 2, -1],
                [4, 4, 1],
                [4, 4, -1],
                [1, 1, 1],
            ],
            "expected": [
                {"resources": 2, "EARs": {0: [0], 1: [0]}},
                {"resources": 1, "EARs": {2: [1, 3]}},
                {"resources": 4, "EARs": {0: [1, 2], 1: [1, 2]}},
                {"resources": 1, "EARs": {0: [3], 1: [3]}},
            ],
        },
        {
            "resources": [
                [2, 2, -1],
                [4, 4, 1],
                [4, 8, -1],
                [1, 1, 1],
            ],
            "expected": [
                {"resources": 2, "EARs": {0: [0], 1: [0]}},
                {"resources": 1, "EARs": {2: [1, 3]}},
                {"resources": 4, "EARs": {0: [1, 2], 1: [1]}},
                {"resources": 8, "EARs": {1: [2]}},
                {"resources": 1, "EARs": {0: [3], 1: [3]}},
            ],
        },
        {
            "resources": [
                [2, 2, -1],
                [4, 4, 1],
                [4, -1, -1],
                [1, 1, 1],
            ],
            "expected": [
                {"resources": 2, "EARs": {0: [0], 1: [0]}},
                {"resources": 1, "EARs": {2: [1, 3]}},
                {"resources": 4, "EARs": {0: [1, 2], 1: [1]}},
                {"resources": 1, "EARs": {0: [3], 1: [3]}},
            ],
        },
    )
    for i in examples:
        jobscripts_i, _ = allocate_jobscripts(i["resources"])
        assert jobscripts_i == i["expected"]
