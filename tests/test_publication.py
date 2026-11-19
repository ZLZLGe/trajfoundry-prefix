from copy import deepcopy
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from botocore.awsrequest import AWSResponse
from botocore.exceptions import ClientError
from botocore.utils import parse_timestamp

from trajectory_prefix.publication import (
    PublicationStore,
    _node_for_reviewed_token_estimate,
    _parse_s3_timestamp,
    encoded,
    merge_origins,
    model_component,
    normalized_manifest,
    selected,
    selection_enabled,
    selection_manifest,
    sha256,
)
from trajectory_prefix.publication_job import (
    publication_counts,
    require_inventory,
    source_inventory,
    write_sorted_lineage,
)


def test_reviewed_estimate_only_changes_the_validation_copy():
    node = Mock(total_tokens=300)
    review = {"sha256": "a" * 64, "stored_total_tokens": 300, "recomputed_total_tokens": 275}
    ref = {"sha256": "a" * 64}
    result = _node_for_reviewed_token_estimate(node, ref, review)
    assert result is node.model_copy.return_value
    node.model_copy.assert_called_once_with(update={"total_tokens": 275})
    assert node.total_tokens == 300
    for change in ({"sha256": "b" * 64}, {"stored_total_tokens": 301},
                   {"recomputed_total_tokens": True}, {"recomputed_total_tokens": -1},
                   {"recomputed_total_tokens": 300}):
        with pytest.raises(ValueError, match="Reviewed token estimate"):
            _node_for_reviewed_token_estimate(node, ref, review | change)


def test_sorted_lineage_preserves_validated_bytes_and_empty_partitions(tmp_path):
    parts = tmp_path / "sorted"
    parts.mkdir()
    target = tmp_path / "lineage.jsonl"
    first = {"trajectory_id": "1" * 64, "representative": "a", "origin_count": 1, "origins": [
        {"source_ref": "source/a", "sha256": "a" * 64},
    ]}
    second = {"trajectory_id": "2" * 64, "representative": "乙", "origin_count": 1, "origins": [
        {"source_ref": '中文/"example"\n', "sha256": "b" * 64, "source_byte_offset": 2**80, "disposition": None},
    ]}
    (parts / "part-00002").write_bytes(encoded({"path": "乙.jsonl", "lineage": second}))
    (parts / "part-00001").touch()
    (parts / "part-00000").write_bytes(encoded({"path": "a.jsonl", "lineage": first}))
    validated = []
    identities = [("a.jsonl", first["trajectory_id"]), ("乙.jsonl", second["trajectory_id"])]
    write_sorted_lineage(parts, target, iter(identities), validated.append)
    assert validated == [first, second]
    assert target.read_bytes() == encoded(first) + encoded(second)

    empty = tmp_path / "empty"
    empty.mkdir()
    (empty / "part-00000").touch()
    write_sorted_lineage(empty, target, iter(()), validated.append)
    assert target.read_bytes() == b""


@pytest.mark.parametrize("actual", [
    [("a", "1")],
    [("a", "1"), ("b", "2"), ("x", "3")],
    [("a", "1"), ("a", "1")],
    [("b", "2"), ("a", "1")],
    [("a", "2"), ("b", "1")],
])
def test_sorted_lineage_rejects_missing_duplicate_reordered_or_wrong_roots(tmp_path, actual):
    target = tmp_path / "lineage.jsonl"
    target.write_bytes(b"previous output\n")
    (tmp_path / "part-00000").write_bytes(b"".join(
        encoded({"path": path, "lineage": {"trajectory_id": identifier}}) for path, identifier in actual
    ))
    with pytest.raises(ValueError, match="Sorted lineage"):
        write_sorted_lineage(tmp_path, target, [("a", "1"), ("b", "2")], lambda row: None)
    assert target.read_bytes() == b"previous output\n" and not list(tmp_path.glob("*.tmp"))


def test_publication_counts_keep_raw_failures_and_exclude_removed_metrics():
    rows = [
        {"status": "kept", "strict": True, "selected": True, "model": "b", "reason_codes": []},
        {"status": "kept", "strict": False, "selected": False, "model": "a", "reason_codes": ["shared"]},
        {"status": "removed", "strict": False, "selected": True, "model": "c",
         "reason_codes": ["shared", "removed"]},
    ]
    source = {"reason_counts": {"shared": 4, "removed": 1, "raw_only": 2, "zero": 0},
              "input_files": 10, "duplicate_trajectories": 1}
    counts, models, eligible = publication_counts(iter(rows), source, 3)
    assert counts == {"reason_counts": {"raw_only": 2, "shared": 3}, "input_files": 10,
                      "duplicate_trajectories": 3, "accepted": 1, "quarantined_trajectories": 1}
    assert models == {"a": 1, "b": 1} and eligible == 1
    with pytest.raises(ValueError, match="Source reason accounting"):
        publication_counts(iter(rows), {**source, "reason_counts": {"shared": 1, "removed": 1}}, 3)

    counts, models, eligible = publication_counts(iter(()), {"reason_counts": {"raw_only": 2}}, 0)
    assert counts == {"reason_counts": {"raw_only": 2}, "accepted": 0, "quarantined_trajectories": 0,
                      "duplicate_trajectories": 0}
    assert models == {} and eligible == 0


@pytest.mark.parametrize("value", [
    "2026-11-19T02:01:02Z", "2026-11-19T02:01:02.000Z", "2026-11-19T02:01:02.123456Z",
    "2026-11-19T02:01:02.123456789Z", "2026-11-19T02:01:02+03:30",
    "Thu, 19 Nov 2026 02:01:02 GMT", 1234567890, "1234567890",
])
def test_publication_timestamp_matches_sdk(value):
    assert _parse_s3_timestamp(value) == parse_timestamp(value)


def test_invalid_s3_utc_timestamp_keeps_sdk_error():
    value = "2026-02-30T00:00:00Z"
    with pytest.raises(ValueError) as baseline:
        parse_timestamp(value)
    with pytest.raises(type(baseline.value)):
        _parse_s3_timestamp(value)


def test_bulk_inventory_parser_is_local_to_the_driver_client(spec, monkeypatch):
    timestamp = "2026-11-19T02:01:02.123Z"
    xml = (f"<ListBucketResult><Contents><Key>a&amp;b</Key><Size>12</Size>"
           f"<ETag>etag</ETag><LastModified>{timestamp}</LastModified></Contents></ListBucketResult>").encode()
    raw = SimpleNamespace(stream=lambda: iter([xml]))
    seen = []

    def timestamp_parser(value):
        seen.append(value)
        return _parse_s3_timestamp(value)

    def respond(request, **_):
        return AWSResponse(request.url, 200, {"content-type": "application/xml"}, raw)

    monkeypatch.setattr("trajectory_prefix.publication._parse_s3_timestamp", timestamp_parser)
    monkeypatch.setattr("trajectory_prefix.publication.credentials",
                        lambda _: {"aws_access_key_id": "test", "aws_secret_access_key": "test"})
    settings = {**spec, "s3_endpoint": "http://localhost:9", "s3_region": "us-east-1",
                "s3_credentials_file": "unused"}
    for bulk in (True, False):
        store = PublicationStore(settings, "owner", bulk_inventory=bulk)
        try:
            store.client.meta.events.register("before-send.s3.ListObjectsV2", respond)
            page = store.client.list_objects_v2(Bucket=store.bucket, Prefix=store.normalized)
            assert page["Contents"] == [{"Key": "a&b", "Size": 12, "ETag": "etag",
                                         "LastModified": parse_timestamp(timestamp)}]
        finally:
            store.client.close()
    assert seen == [timestamp]


def inventory_client(objects, starts, mode="ordered"):
    def paginate(*, Bucket, Prefix, StartAfter=None):
        starts.append(StartAfter)
        names = sorted(objects)
        if StartAfter is not None:
            if mode in {"NotImplemented", "AccessDenied"}:
                raise ClientError({"Error": {"Code": mode}}, "ListObjectsV2")
            if mode != "ignore_after":
                names = [name for name in names if Prefix + name > StartAfter]
        if mode == "unordered":
            names.reverse()
        for pos in range(0, len(names), 2):
            yield {"Contents": [{"Key": Prefix + name, "Size": objects[name]} for name in names[pos:pos + 2]]}
    return SimpleNamespace(get_paginator=lambda operation: SimpleNamespace(paginate=paginate))


@pytest.mark.parametrize("change", [
    "intact", "optional_manifest_absent", "extra_before", "extra_between", "extra_after",
    "missing_boundary", "missing_last", "same_total", "size",
])
def test_partition_inventory_checks_the_entire_keyspace(spec, monkeypatch, change):
    monkeypatch.setattr("trajectory_prefix.publication_job._INVENTORY_KEYS_PER_RANGE", 2)
    expected = {"a": 0, "b": 1, "c": 2, "d": 3, "lineage.jsonl": 4, "轨迹": 5}
    actual = {**expected, "manifest.json": 7}
    if change == "optional_manifest_absent":
        del actual["manifest.json"]
    elif change in {"extra_before", "extra_between", "extra_after"}:
        actual[{"extra_before": "!", "extra_between": "b/nested", "extra_after": "轨迹2"}[change]] = 0
    elif change == "missing_boundary":
        del actual["b"]
    elif change == "missing_last":
        del actual["轨迹"]
    elif change == "same_total":
        actual["b/new"] = actual.pop("b")
    elif change == "size":
        actual["c"] += 1
    starts = []
    store = PublicationStore(spec, "test", client=inventory_client(actual, starts))
    if change in {"intact", "optional_manifest_absent"}:
        require_inventory(store, store.normalized, expected)
        assert set(starts) == {None, store.normalized + "b", store.normalized + "d"}
    else:
        with pytest.raises(ValueError, match="File collection or byte counts"):
            require_inventory(store, store.normalized, expected)


@pytest.mark.parametrize("mode", ["ordered", "NotImplemented", "ignore_after", "unordered", "AccessDenied"])
def test_source_inventory_requires_manifest_and_supports_legacy_listings(spec, monkeypatch, mode):
    monkeypatch.setattr("trajectory_prefix.publication_job._INVENTORY_KEYS_PER_RANGE", 2)
    expected = {"a": 2, "b": 3, "lineage.jsonl": 4, "manifest.json": 0}
    actual = dict(expected)
    starts = []
    store = PublicationStore(spec, "test", client=inventory_client(actual, starts, mode))
    if mode == "AccessDenied":
        with pytest.raises(ClientError, match="AccessDenied"):
            require_inventory(store, store.source, expected, allow_manifest=False)
        return
    require_inventory(store, store.source, expected, allow_manifest=False)
    assert starts.count(None) == (1 if mode == "ordered" else 2)
    del actual["manifest.json"]
    with pytest.raises(ValueError, match="File collection or byte counts"):
        require_inventory(store, store.source, expected, allow_manifest=False)


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


def test_archive_origins_preserve_repeated_bytes_at_distinct_offsets():
    a = {"source_ref": "archive.jsonl", "source_byte_offset": 0, "sha256": "a" * 64, "reason_codes": []}
    b = {**a, "source_byte_offset": 57}
    assert merge_origins([b, a, b]) == [a, b]
    with pytest.raises(ValueError, match="Conflicting"):
        merge_origins([b, {**b, "sha256": "b" * 64}])


def test_published_archive_preserves_raw_counts_and_recomputes_retained_totals():
    from trajectory_prefix.archive_publication import published_archive

    short = {"strict": True, "metrics": {"total_rounds": 2, "total_tokens": 90, "total_tool_calls": 1},
             "archive": {"messages": 4, "rewrites": {"decoded": 2}, "success": True}}
    long = {"strict": False, "metrics": {"total_rounds": 3, "total_tokens": 120, "total_tool_calls": 1},
            "archive": {"messages": 6, "rewrites": {"decoded": 4}, "success": False}}
    before = {"totals": {"input_rows": 3, "physical_rows": 3, "duplicate_rows": 1, "trajectories": 2,
                         "total_rounds": 5, "total_tokens": 210, "total_tool_calls": 2, "messages": 10,
                         "accepted": 1, "quarantined": 1, "source_success_true": 1, "source_success_false": 1,
                         "raw_source_success_true": 2, "raw_source_success_false": 1},
              "rewrites": {"decoded": 6}, "inventory": [{"key": "archive.jsonl", "bytes": 500}]}
    after = published_archive(before, [short, long], [long])
    assert after["totals"] == {**before["totals"], "duplicate_rows": 2, "trajectories": 1, "total_rounds": 3,
                               "total_tokens": 120, "total_tool_calls": 1, "messages": 6,
                               "accepted": 0, "source_success_true": 0}
    assert after["rewrites"] == {"decoded": 4} and after["inventory"] == before["inventory"]
    with pytest.raises(ValueError, match="archive manifest"):
        published_archive(before, [short], [short])


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


def test_normalized_only_has_no_selection_targets_or_required_thresholds(spec):
    minimal = {k: spec[k] for k in ("source_uri", "normalized_uri")}
    minimal["publish_selection"] = False
    store = PublicationStore(minimal, "owner", client=object())
    assert not selection_enabled(minimal)
    assert store.targets == [store.normalized]
    assert store.selections == {}
    assert store.manifest_keys == {store.normalized + "manifest.json"}
    for operation, key in [("PutObject", "dataset/selected/model-a/v003/manifest.json"),
                           ("CopyObject", "dataset/selected/model-a/v003/dt=x/a.jsonl")]:
        with pytest.raises(ValueError, match="outside publication scope"):
            store.guard(SimpleNamespace(name=operation), {
                "Bucket": "bucket", "Key": key,
                "CopySource": {"Bucket": "bucket", "Key": store.source + "a.jsonl"},
                "CopySourceIfMatch": "etag"})
    store.guard(SimpleNamespace(name="PutObject"), {
        "Bucket": "bucket", "Key": store.normalized + "manifest.json"})
    assert selection_enabled(spec)
    for value in (0, 1, "false", None):
        with pytest.raises(ValueError, match="boolean"):
            PublicationStore({**minimal, "publish_selection": value}, "owner", client=object())


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


def test_witness_reads_reuse_one_verified_file_per_task_with_a_size_limit(spec, monkeypatch):
    monkeypatch.setattr("trajectory_prefix.publication._MAX_CACHED_WITNESS_BYTES", 5)
    client = FakeStore(b"valid")
    client.get_object = Mock(wraps=client.get_object)
    store = PublicationStore(spec, "owner", client=client)
    a = {"path": "a.jsonl", "bytes": 5, "sha256": sha256(b"valid")}
    b = {**a, "path": "b.jsonl"}
    for target in (a, dict(a), b, a):
        assert store.read_witness(target) == b"valid"
    assert [call.kwargs["Key"] for call in client.get_object.call_args_list] == [
        store.source + path for path in ("a.jsonl", "b.jsonl", "a.jsonl")
    ]
    client.data = b"larger"
    large = {"path": "large.jsonl", "bytes": 6, "sha256": sha256(b"larger")}
    for _ in range(2):
        assert store.read_witness(large) == b"larger"
    client.data = b"valid"
    assert store.read_witness(a) == b"valid"
    other_task = PublicationStore(spec, "owner", client=client)
    assert other_task.read_witness(a) == b"valid"
    assert client.get_object.call_count == 7


@pytest.mark.parametrize("mismatch", [{"sha256": sha256(b"other")}, {"bytes": 4}])
def test_witness_read_validates_changed_digest_or_length_and_can_retry(spec, mismatch):
    client = FakeStore(b"valid")
    client.get_object = Mock(wraps=client.get_object)
    store = PublicationStore(spec, "owner", client=client)
    target = {"path": "a.jsonl", "bytes": 5, "sha256": sha256(b"valid")}
    assert store.read_witness(target) == b"valid"
    with pytest.raises(ValueError, match="differs from frozen input"):
        store.read_witness({**target, **mismatch})
    client.data = b"wrong"
    with pytest.raises(ValueError, match="differs from frozen input"):
        store.read_witness(target)
    client.data = b"valid"
    assert store.read_witness(target) == b"valid"
    assert client.get_object.call_count == 4


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
