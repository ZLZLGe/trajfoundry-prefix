from copy import deepcopy
from io import BytesIO
from types import SimpleNamespace

import pytest

from trajectory_prefix.publication import (
    PublicationStore,
    encoded,
    merge_origins,
    model_component,
    normalized_manifest,
    selected,
    selection_manifest,
    sha256,
)
from trajectory_prefix.publication_job import require_inventory, source_inventory


@pytest.fixture
def spec():
    return {"source_uri": "s3://bucket/dataset/normalized/v003/dt=2026-08-21/",
            "normalized_uri": "s3://bucket/dataset/normalized/v004/dt=2026-08-21/",
            "selection_base_uri": "s3://bucket/dataset/selected/", "selection_version": "v002",
            "models": ["model-a", "provider/model-b"],
            "min_tokens": 50000, "min_tool_calls": 20, "min_assistant_steps": 20}


def test_inclusive_thresholds_and_no_quality_gate(spec):
    row = {"total_tokens": 50000, "total_tool_calls": 20, "total_rounds": 20,
           "tool_defs_tag": "incomplete", "normalization_audit": {"tag": "quarantined"}}
    assert selected(row, spec)
    for field in ("total_tokens", "total_tool_calls", "total_rounds"):
        assert not selected({**row, field: row[field] - 1}, spec)
        with pytest.raises(ValueError):
            selected({**row, field: True}, spec)


def test_lineage_retains_all_sources_and_merges_only_compatible_origins():
    a = {"source_ref": "a", "sha256": "a" * 64, "captured_at": "date",
         "disposition": "pass", "reason_codes": []}
    b = {**a, "source_ref": "b", "sha256": "b" * 64}
    dirty = {**a, "disposition": "quarantined", "reason_codes": ["missing_tool_result"]}
    before = deepcopy([a, b, dirty])
    result = merge_origins([a, b, dirty, a])
    assert [x["source_ref"] for x in result] == ["a", "b"]
    assert result[0]["disposition"] == "quarantined"
    assert result[0]["reason_codes"] == ["missing_tool_result"]
    assert [a, b, dirty] == before
    with pytest.raises(ValueError, match="Conflicting"):
        merge_origins([a, {**a, "sha256": "c" * 64}])
    with pytest.raises(ValueError, match="Conflicting"):
        merge_origins([a, {**a, "captured_at": "changed"}])


def test_manifest_preserves_raw_failures_and_excludes_lineage_bytes(spec):
    old = {"parse_failure_count": 67, "input_root": "s3://bucket/raw/",
           "sessionless_partitioning": "normalized-bytes-v1"}
    files = [{"path": "a.jsonl", "bytes": 200}, {"path": "lineage.jsonl", "bytes": 900}]
    counts = {"accepted": 0, "quarantined_trajectories": 1, "input_files": 5}
    manifest = normalized_manifest(old, files, counts, {
        "created_at": "now", "started_at_ms": 1, "completed_at_ms": 2, "publication_id": "abc"})
    assert manifest["total_bytes"] == 200
    assert manifest["parse_failure_count"] == 67
    assert manifest["record_count"] == 1
    assert manifest["input_root"] == old["input_root"]
    result = selection_manifest("model-a", [{"date": "2026-08-21", "bytes": 200}], spec)
    assert len(result) == 9 and result["source"].endswith("normalized/v004/")


def test_writer_rejects_old_versions_and_unguarded_copies(spec):
    store = PublicationStore(spec, "owner", client=object())
    assert store.selections["model-a"] == "dataset/selected/model-a/v002/"
    for operation, params in [
        ("PutObject", {"Key": "dataset/selected/model-a/v001/manifest.json"}),
        ("DeleteObject", {"Key": store.normalized + "a.jsonl"}),
        ("CopyObject", {"Key": store.normalized + "a.jsonl", "CopySource": {
            "Bucket": "bucket", "Key": store.source + "a.jsonl"}}),
    ]:
        with pytest.raises(ValueError):
            store.guard(SimpleNamespace(name=operation), {"Bucket": "bucket", **params})
    with pytest.raises(ValueError, match="collision"):
        PublicationStore({**spec, "models": ["a/b", "a_b"]}, "owner", client=object())
    with pytest.raises(ValueError):
        model_component("..")


class Body(BytesIO):
    def iter_chunks(self, chunk_size):
        while block := self.read(chunk_size):
            yield block


class FakeStore:
    def __init__(self, data, owner="owner"):
        self.data, self.owner = data, owner
        self.copies = []

    def head_object(self, **kwargs):
        return {"Metadata": {"publication-id": self.owner, "sha256": sha256(b"valid")},
                "ContentLength": 5, "ETag": "etag"}

    def get_object(self, **kwargs):
        return {"Body": Body(self.data), "ETag": "etag"}


def test_retry_rechecks_bytes_and_rejects_foreign_owner(spec):
    expected = {"sha256": sha256(b"valid"), "bytes": 5}
    store = PublicationStore(spec, "owner", client=FakeStore(b"valid"))
    assert store.copy(store.source + "a.jsonl", store.normalized + "a.jsonl", expected, "src") == "etag"
    corrupt = PublicationStore(spec, "owner", client=FakeStore(b"wrong"))
    with pytest.raises(ValueError, match="hash mismatch"):
        corrupt.copy(corrupt.source + "a.jsonl", corrupt.normalized + "a.jsonl", expected, "src")
    foreign = PublicationStore(spec, "owner", client=FakeStore(b"valid", owner="other"))
    with pytest.raises(ValueError, match="collision"):
        foreign.copy(foreign.source + "a.jsonl", foreign.normalized + "a.jsonl", expected, "src")


def test_source_inventory_includes_only_frozen_diagnostics(tmp_path, spec):
    source = {"files": [{"path": "a.jsonl", "bytes": 200},
                        {"path": "lineage.jsonl", "bytes": 900}]}
    receipt = {"source_uri": spec["source_uri"], "source_manifest_sha256": "m",
               "input_plan_sha256": "p", "complete_requested_prefix": True,
               "input_count": 1, "input_bytes": 200,
               "non_trajectory_files": [{"path": "diagnostics/input.ndjson", "bytes": 300},
                                        {"path": "lineage.jsonl", "bytes": 900},
                                        {"path": "manifest.json", "bytes": 100}]}
    receipt_path = tmp_path / "inventory.json"
    receipt_path.write_bytes(encoded(receipt))
    spec = {**spec, "source_manifest_sha256": "m", "input_plan_sha256": "p",
            "source_inventory_receipt": str(receipt_path),
            "source_inventory_sha256": sha256(receipt_path.read_bytes())}
    expected = source_inventory(spec, source, 100)
    assert expected == {"a.jsonl": 200, "lineage.jsonl": 900, "manifest.json": 100,
                        "diagnostics/input.ndjson": 300}
    store = SimpleNamespace(inventory=lambda prefix: dict(expected))
    require_inventory(store, "source/", expected, allow_manifest=False)
    for changed in ({**expected, "unrecorded.jsonl": 200},
                    {**expected, "diagnostics/unrecorded.ndjson": 10},
                    {**expected, "diagnostics/input.ndjson": 301}):
        store = SimpleNamespace(inventory=lambda prefix: changed)
        with pytest.raises(ValueError, match="File collection"):
            require_inventory(store, "source/", expected, allow_manifest=False)
    with pytest.raises(ValueError, match="ancillary"):
        source_inventory(spec, source, 101)
    receipt_path.write_bytes(encoded({**receipt, "input_count": 2}))
    with pytest.raises(ValueError, match="receipt changed"):
        source_inventory(spec, source, 100)
