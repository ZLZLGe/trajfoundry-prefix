import hashlib
import importlib.util
import json
from pathlib import Path

import pytest
from test_tool_ids import sample

from trajectory_prefix.projection import Policy
from trajectory_prefix.storage import StorageConfig, reference

spec = importlib.util.spec_from_file_location(
    "prefix_audit", Path(__file__).parents[1] / "scripts/audit.py"
)
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


def input_ref(tmp_path, name, node):
    data = json.dumps(node).encode()
    path = tmp_path / name
    path.write_bytes(data)
    return reference(
        {
            "dataset": "sample",
            "date": "archive",
            "path": name,
            "local_path": str(path),
            "sha256": hashlib.sha256(data).hexdigest(),
            "bytes": len(data),
        }
    )


def test_audit_proves_actual_output_and_rejects_cache_as_missing_output(tmp_path):
    a = input_ref(tmp_path, "a.json", sample("a"))
    b = input_ref(tmp_path, "b.json", sample("b", True))
    source = tmp_path / "run"
    output = source / "trajectories" / b["id"]
    output.parent.mkdir(parents=True)
    output.write_bytes(Path(b["local_path"]).read_bytes())
    decisions = [
        json.dumps({"status": "removed", "record": a, "retained": b}),
        json.dumps({"status": "kept", "record": b}),
    ]
    counts = dict(audit.verify_partition(decisions, str(source), StorageConfig(), Policy()))
    assert counts[("all", "witness_verified")] == counts[("all", "output_hash_verified")] == 1
    assert counts[("all", "required:tool_id_alpha")] == 1
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / b["sha256"]).write_bytes(output.read_bytes())
    output.unlink()
    with pytest.raises(ValueError, match="unavailable locally"):
        list(
            audit.verify_partition(
                decisions, str(source), StorageConfig(cache_dir=str(cache)), Policy()
            )
        )


def test_audit_rejects_incorrect_semantic_witness_even_with_valid_hashes(tmp_path):
    a = input_ref(tmp_path, "a.json", sample("a"))
    node = sample("b", True)
    node["messages"][1]["tool_calls"][0]["function"]["arguments"] = {"cmd": "delete"}
    b = input_ref(tmp_path, "b.json", node)
    source = tmp_path / "run"
    output = source / "trajectories" / b["id"]
    output.parent.mkdir(parents=True)
    output.write_bytes(Path(b["local_path"]).read_bytes())
    line = json.dumps({"status": "removed", "record": a, "retained": b})
    with pytest.raises(RuntimeError, match="witness verification failed"):
        list(audit.verify_partition([line], str(source), StorageConfig(), Policy()))
