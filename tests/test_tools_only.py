from copy import deepcopy

import pytest
from test_engine import assistant, node, record, user

from trajectory_prefix.engine import deduplicate
from trajectory_prefix.projection import Policy, covers, project
from trajectory_prefix.verification import raw_covers

REF = {"dataset": "sample", "date": "archive"}


@pytest.mark.parametrize(
    "extra",
    [
        {
            "sub_agent_trajectory": {
                "child": {"messages": [{"role": "assistant", "content": "child"}]}
            }
        },
        {"compaction_items": [{"item": {"content": "summary"}}]},
        {"multimodal_file_mapping": [{"part_id": "p", "object_name": "image"}]},
        {"agent_messages": [{"origin": "response", "item": {"text": "subtask"}}]},
        {"server_tool_calls": [{"name": "search"}]},
        {"instructions": "top-level instructions"},
        {"custom_extension": "retained in original files"},
    ],
)
def test_only_tool_definitions_limit_auxiliary_coverage(extra):
    a = node([user(), assistant()], **extra)
    b = node([user(), assistant(), user("continue"), assistant("done")])
    original = deepcopy((a, b))
    assert covers(project(a, "sample", "archive"), project(b, "sample", "archive"))
    assert raw_covers(a, b, REF, REF)
    strict = Policy(extra_information="preserve_all")
    assert not covers(
        project(a, "sample", "archive", strict), project(b, "sample", "archive", strict)
    )
    assert not raw_covers(a, b, REF, REF, strict)
    assert (a, b) == original


def test_tool_parameter_schema_must_be_contained_even_if_messages_match():
    a, b = node([user(), assistant()]), node([user(), assistant()])
    a["tools"] = [
        {"type": "function", "function": {"name": "exec", "parameters": {"type": "object"}}}
    ]
    assert not raw_covers(a, b, REF, REF)
    b["tools"] = deepcopy(a["tools"])
    b["tools"][0]["function"]["parameters"]["required"] = ["command"]
    assert not raw_covers(a, b, REF, REF)
    assert not covers(project(a, "sample", "archive"), project(b, "sample", "archive"))


def test_sub_numbers_do_not_partition_a_real_session_or_merge_real_branches(tmp_path):
    a = node([user(), assistant()])
    b = node(a["messages"] + [user("continue"), assistant("done")])
    branch = node([user(), assistant("a different answer")])
    a["metadata"]["sub_session_id"] = 999
    b["metadata"]["sub_session_id"] = 10001
    rows = deduplicate(
        [
            record("session_sub_999.jsonl", a),
            record("session_sub_10001.jsonl", b),
            record("session_sub_10002.jsonl", branch),
        ],
        tmp_path,
    )
    assert {r["record"]["id"] for r in rows if r["status"] == "kept"} == {
        "session_sub_10001.jsonl",
        "session_sub_10002.jsonl",
    }


def test_source_extensions_are_ignored_but_identity_still_groups_records():
    a, b = node([user(), assistant()]), node([user(), assistant()])
    a["metadata"]["source_metadata"] = {"benchmark": "first", "record_id": "1"}
    b["metadata"]["source_metadata"] = {"benchmark": "second", "record_id": "2"}
    assert raw_covers(a, b, REF, REF)
    b["metadata"]["user_id"] = "someone else"
    assert not raw_covers(a, b, REF, REF)
