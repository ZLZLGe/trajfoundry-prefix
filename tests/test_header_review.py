import copy

import pytest

from trajectory_prefix.header_review import candidate, merge_candidates
from trajectory_prefix.projection import Policy, covers, project


def node(header="instruction A"):
    return {
        "model": "m",
        "harness": "h",
        "metadata": {"session_id": "s", "user_id": "u"},
        "tools": [],
        "messages": [
            {"role": "system", "content": header},
            {"role": "user", "content": "inspect project"},
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {
                        "id": "c0",
                        "type": "function",
                        "function": {"name": "read", "arguments": "{}"},
                    }
                ],
            },
            {"role": "tool", "content": "file text", "tool_call_id": "c0", "name": "read"},
            {"role": "assistant", "content": "done"},
        ],
    }


def entry(identifier):
    return {"dataset": "test", "date": "2026-10-10", "id": identifier}


def test_header_review_does_not_relax_real_coverage():
    a, b = node(), node("instruction B")
    before = copy.deepcopy((a, b))
    ka, va = candidate(entry("a"), a, Policy())
    kb, vb = candidate(entry("b"), b, Policy())
    assert ka == kb and len(merge_candidates(va, vb)["variants"]) == 2
    assert not covers(project(a, "test", "2026-10-10"), project(b, "test", "2026-10-10"))
    assert (a, b) == before


@pytest.mark.parametrize("change", ["user", "arguments", "tools", "model", "session"])
def test_real_body_definition_and_identity_differences_do_not_match(change):
    a, b = node(), node("instruction B")
    if change == "user":
        b["messages"][1]["content"] = "different task"
    elif change == "arguments":
        b["messages"][2]["tool_calls"][0]["function"]["arguments"] = '{"path":"other"}'
    elif change == "tools":
        b["tools"] = [{"name": "read", "description": "different definition"}]
    elif change == "model":
        b["model"] = "other"
    else:
        b["metadata"]["session_id"] = "other"
    assert candidate(entry("a"), a, Policy())[0] != candidate(entry("b"), b, Policy())[0]


def test_bounded_header_reduction_is_associative_and_keeps_distinct_variants():
    values = [
        candidate(entry(str(i)), node(header), Policy())[1]
        for i, header in enumerate(["A", "A", "B", "C"])
    ]
    left = merge_candidates(
        merge_candidates(values[0], values[1]), merge_candidates(values[2], values[3])
    )
    right = merge_candidates(
        values[3], merge_candidates(values[2], merge_candidates(values[1], values[0]))
    )
    assert left == right and left["record_count"] == 4 and len(left["variants"]) == 2
