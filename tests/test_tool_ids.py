from copy import deepcopy

import pytest

from trajectory_prefix.projection import Policy, covers, project


def call(identifier, command="pwd"):
    return {
        "id": identifier,
        "type": "function",
        "function": {"name": "exec", "arguments": {"cmd": command}},
    }


def sample(identifier, result=False):
    messages = [
        {"role": "user", "content": "inspect"},
        {
            "role": "assistant",
            "content": "",
            "reasoning_content": "",
            "tool_calls": [call(identifier)],
        },
    ]
    if result:
        messages += [
            {"role": "tool", "name": "exec", "tool_call_id": identifier, "content": "/workspace"},
            {"role": "assistant", "content": "done", "reasoning_content": ""},
        ]
    return {"metadata": {"session_id": "s"}, "model": "m", "messages": messages}


def test_response_id_rewrite_and_result_reference_preserve_prefix():
    a, b = sample("chatcmpl-tool-old"), sample("call_new", True)
    original = deepcopy([a, b])
    assert not covers(project(a, "x", "d", Policy(())), project(b, "x", "d", Policy(())))
    assert covers(project(a, "x", "d"), project(b, "x", "d"))
    assert [a, b] == original


@pytest.mark.parametrize(
    "edit",
    [
        lambda n: n["messages"][1]["tool_calls"][0]["function"].update(name="other"),
        lambda n: n["messages"][1]["tool_calls"][0]["function"].update(arguments={"cmd": "ls"}),
        lambda n: n["messages"][2].update(content="different output"),
        lambda n: n["messages"][2].update(tool_call_id="unknown"),
    ],
)
def test_call_and_result_changes_do_not_fold(edit):
    a, b = sample("a", True), sample("b", True)
    edit(b)
    assert not covers(project(a, "x", "d"), project(b, "x", "d"))


def test_parallel_identical_calls_keep_distinct_result_binding():
    a = sample("a", True)
    a["messages"][1]["tool_calls"].append(call("b"))
    a["messages"].insert(
        3, {"role": "tool", "name": "exec", "tool_call_id": "b", "content": "second"}
    )
    b = deepcopy(a)
    b["messages"][2]["tool_call_id"] = "b"
    b["messages"][3]["tool_call_id"] = "a"
    assert not covers(project(a, "x", "d"), project(b, "x", "d"))


def test_duplicate_call_ids_are_not_treated_as_valid_bindings():
    a = sample("a", True)
    a["messages"][1]["tool_calls"].append(call("a"))
    p = project(a, "x", "d")
    assert not p.mergeable
    assert p.isolation_reason == "ambiguous_tool_binding"


def test_all_history_ids_may_rename_consistently():
    assert covers(project(sample("old", True), "x", "d"), project(sample("new", True), "x", "d"))
