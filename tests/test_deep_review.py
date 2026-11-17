import hashlib
import json
import random
from collections import defaultdict
from copy import deepcopy

import pytest
from test_tool_ids import sample

from trajectory_prefix.deep_review import (
    make_index,
    partition_key,
    scan_candidates,
    sort_entries,
    verify_candidate,
    verify_mask_partition,
)
from trajectory_prefix.projection import Policy
from trajectory_prefix.storage import StorageConfig, reference

POLICY = Policy(extra_information="messages_only")


def ref(name, node):
    data = json.dumps(node).encode()
    return reference({"dataset": "test", "date": "2026-10-10", "path": name + ".json",
                      "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})


def candidates(nodes):
    partitions = defaultdict(list)
    for name, node in nodes.items():
        for key, value in sort_entries(make_index(ref(name, node), node, POLICY)):
            partitions[partition_key(key) % 11].append((key, value))
    return [row for values in partitions.values() for row in scan_candidates(sorted(values))]


def test_prefix_scan_matches_exhaustive_maximal_set_with_equal_groups():
    rng = random.Random(847)
    sequences = [tuple(rng.randrange(5) for _ in range(rng.randrange(2, 10)))
                 for _ in range(100)]
    sequences += [(1, 2), (1, 2), (1, 2), (1, 2, 3), (1, 2, 3, 4)]
    rows = []
    vectors = []
    for i, sequence in enumerate(sequences):
        vector = b"".join(hashlib.sha256(bytes([x])).digest() for x in sequence)
        vectors.append(vector)
        rows.append((("current", b"scope", vector, -1, str(i)),
                     {"ref": {"id": str(i)}, "scope": b"scope", "roles": "A" * len(sequence)}))
    found = [r for r in scan_candidates(sorted(rows)) if r["kind"] == "coverage_candidate"]
    maximal = {a for a in vectors if not any(len(a) < len(b) and b.startswith(a) for b in vectors)}
    assert len(found) == len(vectors) - len(maximal)
    assert len({r["short"]["id"] for r in found}) == len(found)
    for row in found:
        a, b = int(row["short"]["id"]), int(row["long"]["id"])
        assert a != b and vectors[b].startswith(vectors[a])


def test_format_index_finds_unchanged_short_against_changed_long_and_preserves_inputs():
    a, b = sample("a", True), sample("b", True)
    a["messages"][1]["tool_calls"][0]["function"]["arguments"] = '{"cmd":"pwd","n":1}'
    b["messages"][1]["tool_calls"][0]["function"]["arguments"] = '{ "n": 1, "cmd": "pwd" }'
    b["messages"].append({"role": "user", "content": "next"})
    saved = deepcopy([a, b])
    rows = candidates({"a": a, "b": b})
    assert not any(r["kind"] == "coverage_candidate" and r["mode"] == "current" for r in rows)
    row = next(r for r in rows if r["mode"] == "format" and r["kind"] == "coverage_candidate")
    verified = verify_candidate(row, a, b, POLICY)
    assert verified["verified_full_transformed_prefix"]
    assert not verified["current_rule_covers"]
    assert verified["individual_transforms"]["argument_json"]
    assert [a, b] == saved
    bad = deepcopy(b)
    bad["messages"][2]["content"] = "different result"
    with pytest.raises(RuntimeError, match="did not verify"):
        verify_candidate(row, a, bad, POLICY)


def test_header_probe_finds_different_first_messages_and_different_lengths():
    a, b = sample("a", True), sample("b", True)
    a["messages"].insert(0, {"role": "system", "content": "instruction A"})
    b["messages"].insert(0, {"role": "system", "content": "instruction B"})
    b["messages"].append({"role": "user", "content": "next"})
    rows = candidates({"a": a, "b": b})
    row = next(r for r in rows if r["mode"] == "without_headers")
    assert row["kind"] == "coverage_candidate"
    assert not verify_candidate(row, a, b, POLICY)["current_rule_covers"]


def test_notification_probe_is_explicit_diagnostic_not_current_deletion():
    a, b = sample("a", True), sample("b", True)
    a["messages"].insert(2, {"role": "user", "content": "<task-notification>done</task-notification>"})
    b["messages"].append({"role": "user", "content": "next"})
    row = next(r for r in candidates({"a": a, "b": b}) if r["mode"] == "without_notifications")
    checked = verify_candidate(row, a, b, POLICY)
    assert checked["individual_transforms"]["without_notifications"]
    assert not checked["current_rule_covers"]


def test_cross_identity_probe_does_not_cross_model_and_preserves_original_scope_difference():
    nodes = {}
    for name, model in [("a", "m"), ("b", "m"), ("c", "other")]:
        node = sample(name, True)
        node["metadata"]["session_id"] = name
        node["model"] = model
        node["messages"][0]["content"] = "long history " * 100
        node["messages"] += [{"role": "user", "content": "next"},
                             {"role": "assistant", "content": "end"}]
        nodes[name] = node
    rows = [r for r in candidates(nodes) if r["kind"] == "coverage_candidate"]
    assert len(rows) == 1 and rows[0]["mode"] == "cross_identity"
    row = rows[0]
    name_a, name_b = [r["path"].split(".")[0] for r in (row["short"], row["long"])]
    checked = verify_candidate(row, nodes[name_a], nodes[name_b], POLICY)
    assert checked["scope_differences"] == ["session_or_date"]
    assert not checked["current_rule_covers"]


def test_unsafe_tool_bindings_remain_counted_but_never_enter_candidate_modes():
    node = sample("a", True)
    node["messages"][2]["tool_call_id"] = "missing"
    row = make_index(ref("bad", node), node, POLICY)
    assert row["isolation"] == "unresolved_tool_binding"
    assert row["variants"] == {}


def test_mask_neighbor_below_fifty_percent_is_checked_over_full_sequence(tmp_path):
    a, b = sample("a", True), sample("b", True)
    a["messages"][2]["content"] = 'id: <ID_NUMBER>-d603-4e95'
    b["messages"][2]["content"] = 'id: d3750755-d603-4e95'
    tail = [{"role": "user", "content": "more"}, {"role": "assistant", "content": "next"}]
    a["messages"] += tail * 4
    b["messages"] += tail * 5
    nodes = {"a": a, "b": b}
    row = next(r for r in candidates(nodes) if r["mode"] == "current")
    assert row["kind"] == "near_candidate" and row["first_difference_masked"]
    assert row["common_messages"] == 2
    for side in ("short", "long"):
        r = row[side]
        path = tmp_path / r["path"]
        path.write_text(json.dumps(nodes[r["path"].split(".")[0]]))
        r["local_path"] = str(path)
    checked = list(verify_mask_partition([row], POLICY, StorageConfig()))[0]
    assert checked["mask_prefix_directions"] == [True, False]
    assert checked["diagnostic_only"] and not checked["current_rule_covers"]
    # A later real change must reject the entire-prefix hypothesis even though
    # the first mismatch is still explained by a masking placeholder.
    b["messages"][4]["content"] = "different user task"
    new_ref = ref("b", b)
    path = tmp_path / "b.json"
    path.write_text(json.dumps(b))
    row["long"] = dict(new_ref, local_path=str(path))
    assert list(verify_mask_partition([row], POLICY, StorageConfig()))[0]["mask_prefix_directions"] == [False, False]
