import hashlib
import importlib.util
import json
from pathlib import Path

import pytest
from test_tool_ids import sample

from trajectory_prefix.projection import Policy
from trajectory_prefix.residuals import review_files
from trajectory_prefix.storage import StorageConfig, output_relative_path, reference

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


@pytest.mark.parametrize("layout", ["flat", "hash2"])
def test_audit_proves_actual_output_and_rejects_cache_as_missing_output(tmp_path, layout):
    a = input_ref(tmp_path, "a.json", sample("a"))
    b = input_ref(tmp_path, "b.json", sample("b", True))
    source = tmp_path / "run"
    output = source / output_relative_path(b, layout)
    output.parent.mkdir(parents=True)
    output.write_bytes(Path(b["local_path"]).read_bytes())
    decisions = [
        json.dumps({"status": "removed", "record": a, "retained": b}),
        json.dumps({"status": "kept", "record": b}),
    ]
    counts = dict(audit.verify_partition(decisions, str(source), StorageConfig(), Policy(), layout))
    assert counts[("all", "witness_verified")] == counts[("all", "output_hash_verified")] == 1
    assert counts[("all", "required:tool_id_alpha")] == 1
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / b["sha256"]).write_bytes(output.read_bytes())
    output.unlink()
    with pytest.raises(ValueError, match="unavailable locally"):
        list(
            audit.verify_partition(
                decisions, str(source), StorageConfig(cache_dir=str(cache)), Policy(), layout
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


def test_decision_audit_checks_all_source_bytes_and_ignored_tool_definitions(tmp_path):
    short = sample("a")
    short["tools"] = [{"name": "only_in_short"}]
    a = input_ref(tmp_path, "a.json", short)
    b = input_ref(tmp_path, "b.json", sample("b", True))
    source = tmp_path / "check-only"
    lines = [
        json.dumps({"status": "removed", "record": a, "retained": b}),
        json.dumps({"status": "kept", "record": b}),
    ]
    policy = Policy(extra_information="messages_only")
    counts = dict(audit.verify_partition(
        lines, str(source), StorageConfig(), policy, check_only=True
    ))
    assert counts[("all", "retained_input_hash_verified")] == 1
    assert counts[("all", "witness_verified")] == 1
    assert counts[("all", "requires_ignoring_tool_definitions")] == 1
    assert ("all", "output_hash_verified") not in counts
    assert not source.exists()
    with pytest.raises(RuntimeError, match="witness verification failed"):
        list(audit.verify_partition(
            lines, str(source), StorageConfig(), Policy(), check_only=True
        ))
    Path(b["local_path"]).write_text("corrupt source")
    with pytest.raises(ValueError, match="(hash|Hash|SHA256|length|integrity)"):
        list(audit.verify_partition(
            lines, str(source), StorageConfig(), policy, check_only=True
        ))


def test_decision_audit_rejects_message_changes_despite_ignored_tools(tmp_path):
    a = input_ref(tmp_path, "a.json", sample("a", True))
    altered = sample("b", True)
    altered["messages"][2]["content"] = "different result"
    b = input_ref(tmp_path, "b.json", altered)
    line = json.dumps({"status": "removed", "record": a, "retained": b})
    with pytest.raises(RuntimeError, match="witness verification failed"):
        list(audit.verify_partition(
            [line], str(tmp_path), StorageConfig(),
            Policy(extra_information="messages_only"), check_only=True
        ))


@pytest.mark.parametrize("layout", ["flat", "hash2"])
def test_residual_review_reports_real_message_difference_without_merging(tmp_path, layout):
    source = tmp_path / "run"
    decisions = []
    for name, answer in (("a", "branch one"), ("b", "branch two")):
        node = {
            "metadata": {"session_id": "s"},
            "messages": [
                {"role": "user", "content": "start"},
                {"role": "assistant", "content": "ready"},
                {"role": "user", "content": "next"},
                {"role": "assistant", "content": "done"},
                {"role": "user", "content": "last"},
                {"role": "assistant", "content": answer},
            ],
        }
        ref = input_ref(tmp_path, name + ".json", node)
        dest = source / output_relative_path(ref, layout)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(Path(ref["local_path"]).read_bytes())
        decisions.append(json.dumps({"status": "kept", "record": ref}))
    part = tmp_path / "part-00000"
    part.write_text("\n".join(decisions) + "\n")
    output = tmp_path / "audit"
    counts = dict(review_files([str(part)], str(source), str(output), Policy(), layout))
    assert counts[("all", "retained_reviewed")] == 2
    assert counts[("all", "category:assistant->assistant:content")] == 1
    samples = json.loads((output / "near-prefix-samples/part-00000.json").read_text())
    assert samples[0]["common_messages"] == 5
    assert samples[0]["short_message"]["content"] == "branch one"
