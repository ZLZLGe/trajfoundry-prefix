import hashlib
import itertools
import json
import random
from copy import deepcopy

import pytest
from test_tool_ids import sample

from trajectory_prefix.mask_evidence import explain_partition
from trajectory_prefix.mask_job import POLICY, audit_partition, check_partition, index_partition
from trajectory_prefix.mask_verification import raw_relaxed_covers, string_oracle
from trajectory_prefix.masking import (
    choose_group,
    group_candidates,
    markers,
    message_matches,
    string_matches,
)
from trajectory_prefix.projection import project
from trajectory_prefix.storage import StorageConfig


@pytest.mark.parametrize("pattern,value,expected", [
    ("hello <PERSON>", "hello Alice", True),
    ("secret [redacted]", "secret abc123", True),
    ("host <NEW_MASK_KIND>", "host somewhere", True),
    ("x<SENSITIVE>z", "x<OTHER_TYPE>z", True),
    ("x<PHONE>z", "xz", False),
    ("x<PHONE>z", "x" + "a" * 256 + "z", True),
    ("x<PHONE>z", "x" + "a" * 257 + "z", False),
    ("x<PHONE>z", "xa\nbz", False),
    ("x<PHONE>z", "xa\rbz", False),
    ("file <PACK_PATH>", "file /tmp/a", False),
    ("value <KEY>", "value secret", False),
    ("x***z", "xsecretz", False),
    ("<PERSON>\n<EMAIL>", "Alice\na@b", True),
    ("<PERSON><EMAIL>z", "abz", True),
])
def test_bounded_matching_contract(pattern, value, expected):
    assert string_matches(pattern, value) is expected
    assert string_matches(value, pattern) is expected
    assert string_oracle(pattern, value) is expected


def test_independent_oracle_differential():
    rng = random.Random(1711)
    for _ in range(1500):
        a = "".join(rng.choice(["a", "b", "\n", "<PERSON>", "[MASKED]", "<PATH>"])
                    for _ in range(rng.randint(1, 7)))
        b = "".join(rng.choice(["a", "b", "\n", "xy", "<PHONE>"])
                    for _ in range(rng.randint(1, 9)))
        assert string_matches(a, b, 3) == string_oracle(a, b, 3), (a, b)


def indexed(tmp_path, nodes):
    refs = []
    for i, node in enumerate(nodes):
        data = json.dumps(node).encode()
        path = tmp_path / f"{i}.jsonl"
        path.write_bytes(data)
        refs.append({"dataset": "sxf", "date": "2026-08-21", "path": path.name,
                     "local_path": str(path), "bytes": len(data),
                     "sha256": hashlib.sha256(data).hexdigest()})
    return list(index_partition(refs, StorageConfig()))


def test_first_message_mask_and_ids_get_complete_candidate_and_independent_audit(tmp_path):
    a, b = sample("old", True), sample("new", True)
    a["messages"][0]["content"] = "inspect <PERSON>"
    b["messages"][0]["content"] = "inspect Alice"
    original = deepcopy([a, b])
    rows = indexed(tmp_path, [a, b])
    assert rows[0]["group"] == rows[1]["group"]
    pairs = list(group_candidates((rows[0]["group"], rows)))
    assert len(pairs) == 1 and pairs[0]["different_messages"] == [0]
    checked = list(check_partition(pairs, StorageConfig()))
    assert checked[0]["matched"]
    decisions = list(choose_group((rows[0]["group"], (rows, checked))))
    assert len(list(audit_partition(decisions, StorageConfig()))) == 1
    assert [a, b] == original


@pytest.mark.parametrize("change", [
    lambda n: n["metadata"].update(session_id="other"),
    lambda n: n["messages"][3].update(content="another conclusion"),
    lambda n: n["messages"][2].update(tool_call_id="missing"),
    lambda n: n["messages"][1]["tool_calls"][0]["function"].update(name="other_tool"),
])
def test_mask_does_not_override_identity_binding_or_later_content(change):
    a, b = sample("a", True), sample("b", True)
    a["messages"][0]["content"] = "inspect <PERSON>"
    b["messages"][0]["content"] = "inspect Alice"
    change(b)
    ref = {"dataset": "sxf", "date": "d"}
    assert not raw_relaxed_covers(a, b, ref, ref, POLICY)


def test_mask_cannot_change_tool_name_keys_numeric_types_or_extensions():
    a = {"role": "assistant", "content": "", "tool_calls": [{"id": "a", "type": "function",
         "function": {"name": "<TOOL>", "arguments": {"n": 1, "s": "<SENSITIVE>"}}}]}
    b = deepcopy(a)
    b["tool_calls"][0]["function"]["arguments"]["s"] = "secret"
    assert message_matches(a, b)
    for edit in (
        lambda x: x["tool_calls"][0]["function"].update(name="exec"),
        lambda x: x["tool_calls"][0]["function"]["arguments"].update(n=True),
        lambda x: x.update(name="<PERSON>"),
    ):
        c = deepcopy(b)
        edit(c)
        assert not message_matches(a, c)


def test_no_transitive_mask_chain(tmp_path):
    nodes = [sample(str(i), True) for i in range(3)]
    for node, name in zip(nodes, ("Alice", "<PERSON>", "Bobby")):
        node["messages"][0]["content"] = "inspect " + name
    rows = indexed(tmp_path, nodes)
    pairs = list(group_candidates((rows[0]["group"], rows)))
    checked = [x for x in check_partition(pairs, StorageConfig()) if x["matched"]]
    decisions = list(choose_group((rows[0]["group"], (rows, checked))))
    assert sum(x["status"] == "removed" for x in decisions) == 1
    kept = {x["record"]["id"] for x in decisions if x["status"] == "kept"}
    assert {rows[0]["ref"]["id"], rows[2]["ref"]["id"]} == kept


def test_candidate_filter_covers_all_real_relaxed_pairs(tmp_path):
    nodes = []
    for first, args, result in itertools.product(("one", "<PERSON>"), ("pwd", "[COMMAND]"),
                                                 ("/tmp", "<LOCATION>")):
        node = sample(str(len(nodes)), True)
        node["messages"][0]["content"] = first
        node["messages"][1]["tool_calls"][0]["function"]["arguments"]["cmd"] = args
        node["messages"][2]["content"] = result
        nodes.append(node)
    rows = indexed(tmp_path, nodes)
    pairs = list(group_candidates((rows[0]["group"], rows)))
    actual = {frozenset((x["short"]["id"], x["long"]["id"])) for x in pairs}
    for i, j in itertools.combinations(range(len(rows)), 2):
        a, b = rows[i]["ref"], rows[j]["ref"]
        if raw_relaxed_covers(nodes[i], nodes[j], a, b, POLICY):
            ap, bp = project(nodes[i], "sxf", "d", POLICY), project(nodes[j], "sxf", "d", POLICY)
            if ap.tokens != bp.tokens:
                assert frozenset((a["id"], b["id"])) in actual


def test_many_placeholders_do_not_use_regex_backtracking():
    pattern = "a" + "<PERSON>" * 500 + "b"
    assert string_matches(pattern, "a" + "x" * 501 + "b")
    assert not string_matches(pattern, "a" + "x" * 499 + "b")
    assert not markers("<PACK_PATH> <SCHEMA_PATH> <KEY> <PATH>")


def test_explanation_distinguishes_required_marker_from_unchanged_markup(tmp_path):
    a, b = sample("a", True), sample("b", True)
    a["messages"][0]["content"] = "[KNOWN] inspect /Users/[USER]/a"
    b["messages"][0]["content"] = "[KNOWN] inspect /Users/Alice/a"
    rows = indexed(tmp_path, [a, b])
    pairs = list(group_candidates((rows[0]["group"], rows)))
    decisions = list(choose_group((rows[0]["group"], (rows, pairs))))
    result = list(explain_partition(decisions, StorageConfig()))
    assert result[0]["necessary_labels"] == ["[USER]"]
    assert result[0]["source_mask_labels_suffice"]
    assert string_matches("[KNOWN]", "anything")  # Counterfactual state was restored.
