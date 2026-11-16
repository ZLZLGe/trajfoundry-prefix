from copy import deepcopy

import pytest
from test_tool_ids import sample

from trajectory_prefix.projection import Policy, covers, project
from trajectory_prefix.verification import raw_covers

REF = {"dataset": "sample", "date": "archive"}


def examples():
    a = sample("call", True)
    a["messages"][-1] = {
        "role": "assistant",
        "content": "",
        "reasoning_content": "",
        "tool_calls": [],
    }
    b = deepcopy(a)
    b["messages"][-1:] = [
        {"role": "user", "content": "continue"},
        {"role": "assistant", "content": "done"},
    ]
    return a, b


def test_empty_replay_envelope_is_comparison_only():
    a, b = examples()
    before = deepcopy((a, b))
    assert covers(project(a, "sample", "archive"), project(b, "sample", "archive"))
    assert raw_covers(a, b, REF, REF)
    without = Policy(tuple(r for r in Policy().rules if r != "empty_assistant"))
    assert not covers(
        project(a, "sample", "archive", without), project(b, "sample", "archive", without)
    )
    assert not raw_covers(a, b, REF, REF, without)
    assert (a, b) == before


@pytest.mark.parametrize(
    "extra",
    [
        {"reasoning_content": "thought only"},
        {"reasoning_details": [{"text": "thought"}]},
        {"reasoning": "private thought"},
        {"media": ["image"]},
        {"custom": None},
        {"content": "nonempty answer"},
        {"content": "\u00a0"},
    ],
)
def test_reasoning_content_extensions_and_unicode_spaces_are_not_empty(extra):
    a, b = examples()
    a["messages"][-1].update(extra)
    assert not covers(project(a, "sample", "archive"), project(b, "sample", "archive"))
    assert not raw_covers(a, b, REF, REF)


def test_leading_empty_response_remains_and_first_two_partition_tokens_are_stable():
    a, b = examples()
    a["messages"].insert(1, {"role": "assistant", "content": ""})
    b["messages"].insert(1, {"role": "assistant", "content": ""})
    p, q = project(a, "sample", "archive"), project(b, "sample", "archive")
    assert len(p.tokens) == len(a["messages"]) - 1
    assert p.vector[:64] == q.vector[:64]
    assert covers(p, q) and raw_covers(a, b, REF, REF)


def test_ascii_whitespace_requires_existing_whitespace_rule():
    a, b = examples()
    a["messages"][-1]["content"] = "\n \t"
    assert raw_covers(a, b, REF, REF)
    without = Policy(tuple(r for r in Policy().rules if r != "assistant_trailing_space"))
    assert not raw_covers(a, b, REF, REF, without)
    assert not covers(
        project(a, "sample", "archive", without), project(b, "sample", "archive", without)
    )
