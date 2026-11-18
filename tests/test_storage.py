import hashlib
import json

import pytest

from trajectory_prefix.storage import Reader, StorageConfig, atomic_bytes, reference


@pytest.mark.parametrize("path", ["../secret", "/absolute", "x//y", "x/./y", "x\\y"])
def test_unsafe_input_paths_rejected(path):
    with pytest.raises(ValueError):
        reference({"dataset": "d", "date": "date", "path": path, "sha256": "a" * 64, "bytes": 1})


def test_stale_hash_and_output_conflict_are_rejected(tmp_path):
    path = tmp_path / "input"
    path.write_bytes(b"old")
    entry = {"id": "x", "local_path": str(path), "bytes": 3, "sha256": "a" * 64}
    with pytest.raises(ValueError):
        Reader(StorageConfig()).read(entry)
    target = tmp_path / "target"
    target.write_bytes(b"old")
    with pytest.raises(FileExistsError):
        atomic_bytes(target, b"new", hashlib.sha256(b"new").hexdigest())
    assert target.read_bytes() == b"old"


@pytest.mark.parametrize("start,delta", [(2**64, 1), (-2**63 - 1, -1), (10**400, 1)])
def test_large_tool_argument_integers_stay_distinct_through_scan_and_audit(tmp_path, start, delta):
    from test_tool_ids import sample

    from trajectory_prefix.deep_review import NodeReader
    from trajectory_prefix.engine import Record, deduplicate
    from trajectory_prefix.projection import Policy, project
    from trajectory_prefix.spark import verify_reuse_partition
    from trajectory_prefix.verification import raw_covers

    policy = Policy(extra_information="messages_only")
    refs, nodes, records = [], [], []
    for i, number in enumerate([start, start + delta, start]):
        path = tmp_path / f"{i}.json"
        original = sample("call", True)
        original["messages"][1]["tool_calls"][0]["function"]["arguments"] = {"nested": [number]}
        data = json.dumps(original).encode()
        path.write_bytes(data)
        ref = reference(dict(dataset="sample", date="day", path=path.name, local_path=str(path),
                             bytes=len(data), sha256=hashlib.sha256(data).hexdigest()))
        node = Reader(StorageConfig()).node(ref)
        assert node == NodeReader(StorageConfig()).node(ref) == original
        refs.append(ref)
        nodes.append(node)
        records.append(Record(ref, project(node, ref["dataset"], ref["date"], policy)))
    result = deduplicate(records, tmp_path)
    assert sum(r["status"] == "kept" for r in result) == 2
    assert not raw_covers(nodes[0], nodes[1], refs[0], refs[1], policy)
    assert raw_covers(nodes[0], nodes[2], refs[0], refs[2], policy)
    stale = {"status": "removed", "record": refs[0], "retained": refs[1]}
    with pytest.raises(ValueError, match="no ordinary prefix witness"):
        list(verify_reuse_partition([json.dumps(stale)], policy, StorageConfig()))
