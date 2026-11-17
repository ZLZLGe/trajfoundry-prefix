import itertools
import random

from test_masking import indexed
from test_tool_ids import sample

from trajectory_prefix.mask_job import POLICY
from trajectory_prefix.mask_verification import raw_relaxed_covers
from trajectory_prefix.masking import group_candidates, possible_pair, priority
from trajectory_prefix.refinement_job import compose, direct_audit
from trajectory_prefix.storage import StorageConfig


def test_selective_candidates_equal_exhaustive_filter_with_shorter_histories(tmp_path):
    rng = random.Random(30)
    nodes = []
    for i in range(60):
        n = sample(str(i), True)
        n["messages"][0]["content"] = rng.choice(["a", "b", "<PERSON>"])
        n["messages"][2]["content"] = rng.choice(["x", "y", "[SENSITIVE]"])
        if i % 3 == 0:
            n["messages"].pop()
        nodes.append(n)
    rows = sorted(indexed(tmp_path, nodes), key=priority)
    expected = set()
    for a, b in itertools.combinations(rows, 2):
        if (a["mask_count"] or b["mask_count"]) and possible_pair(a, b):
            expected.add((b["ref"]["id"], a["ref"]["id"]))
    actual = [(r["short"]["id"], r["long"]["id"]) for r in group_candidates(("x", rows))]
    assert set(actual) == expected
    assert len(actual) == len(expected)


def test_composed_mapping_is_rejected_without_direct_raw_witness(tmp_path):
    nodes = [sample(str(i), True) for i in range(3)]
    for n, name in zip(nodes, ("Alice", "[USER]", "Bobby")):
        n["messages"][0]["content"] = name
    refs = [r["ref"] for r in indexed(tmp_path, nodes)]
    assert raw_relaxed_covers(nodes[0], nodes[1], refs[0], refs[1], POLICY)
    assert raw_relaxed_covers(nodes[1], nodes[2], refs[1], refs[2], POLICY)
    row = compose(({"record": refs[0], "retained": refs[1], "status": "removed"},
                   {"record": refs[1], "retained": refs[2], "status": "removed", "model": "test"}))
    assert list(direct_audit([row], StorageConfig()))[0]["passed"] is False
