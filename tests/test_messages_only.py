from copy import deepcopy

import pytest
from test_engine import assistant, node, user
from test_tool_ids import sample

from trajectory_prefix.engine import Record, deduplicate
from trajectory_prefix.projection import Policy, covers, project
from trajectory_prefix.verification import raw_covers

POLICY = Policy(extra_information="messages_only")
REF = {"dataset": "sample", "date": "2026-10-10"}


def agrees(a, b, expected):
    assert covers(project(a, "sample", "2026-10-10", POLICY),
                  project(b, "sample", "2026-10-10", POLICY)) is expected
    assert raw_covers(a, b, REF, REF, POLICY) is expected


def test_missing_or_changed_definitions_do_not_block_complete_message_prefix():
    a, b = sample("old", True), sample("new", True)
    a.update(tools=[{"function": {"name": "exec", "parameters": {"required": ["cmd"]}}}],
             instructions="extra instruction", custom_extension="retained only in original")
    b["messages"] += [user("continue"), assistant("done")]
    original = deepcopy((a, b))
    assert not raw_covers(a, b, REF, REF)
    agrees(a, b, True)
    b["tools"] = [{"function": {"name": "exec", "parameters": {"required": ["other"]}}}]
    assert not raw_covers(a, b, REF, REF)
    agrees(a, b, True)
    b.pop("tools")
    assert (a, b) == original


@pytest.mark.parametrize("edit", [
    lambda n: n["messages"][0].update(content="different question"),
    lambda n: n["messages"][1]["tool_calls"][0]["function"].update(arguments={"cmd": "rm"}),
    lambda n: n["messages"][2].update(content="different result"),
    lambda n: n["messages"][2].update(tool_call_id="unbound"),
    lambda n: n["messages"][3].update(role="user"),
    lambda n: n["messages"][3].update(unknown_payload={"preserve": True}),
    lambda n: n.update(model="another model"),
    lambda n: n["metadata"].update(session_id="another session"),
    lambda n: n.update(normalization_audit={"issues": [{"code": "metadata_user_id_masked"}]}),
])
def test_messages_and_identity_protections_remain_in_force(edit):
    a, b = sample("old", True), sample("new", True)
    agrees(a, b, True)
    edit(b)
    agrees(a, b, False)


def test_system_instructions_inside_messages_still_prevent_merging():
    a, b = sample("old", True), sample("new", True)
    a["messages"].insert(0, {"role": "system", "content": "write code"})
    b["messages"].insert(0, {"role": "system", "content": "review code"})
    agrees(a, b, False)


def test_unconstrained_asset_chain_resolves_to_a_final_message_witness(tmp_path):
    a = node([user(), assistant()])
    b = node(a["messages"] + [user("continue"), assistant("middle")])
    c = node(b["messages"] + [user("last"), assistant("done")])
    branch = node([user(), assistant("a different answer")])
    a["tools"] = [{"name": "only_in_short"}]
    b["tools"] = [{"name": "only_in_middle"}]
    nodes = dict(zip("abcd", (a, b, c, branch)))
    rows = deduplicate([
        Record({"id": name, "bytes": 100}, project(value, "sample", "2026-10-10", POLICY))
        for name, value in nodes.items()
    ], tmp_path)
    assert {row["record"]["id"] for row in rows if row["status"] == "kept"} == {"c", "d"}
    for row in rows:
        if row["status"] == "removed":
            assert row["retained"]["id"] == "c"
            assert raw_covers(nodes[row["record"]["id"]], c, REF, REF, POLICY)
