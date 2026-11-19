"""Publish verified decisions without changing trajectory bytes or comparison rules.

The optional publication job uses a pinned TrajFoundry validation package supplied
by its submitter. The ordinary Reader remains strictly read-only.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit

from .comparison import require_prefix
from .projection import Policy
from .storage import credentials, relative_path
from .verification import raw_covers


def encoded(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            + "\n").encode()


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def location(uri):
    parsed = urlsplit(uri)
    if (parsed.scheme != "s3" or not parsed.netloc or parsed.query or parsed.fragment
            or parsed.username or parsed.password):
        raise ValueError("Expected an uncredentialed S3 directory URI")
    prefix = relative_path(parsed.path.strip("/")) + "/"
    return parsed.netloc, prefix


def model_component(model):
    if not isinstance(model, str) or not model:
        raise ValueError("Missing model")
    component = relative_path(model.replace("/", "_"))
    if "/" in component:
        raise ValueError("Model must be one directory component")
    return component


def selected(metrics, spec):
    names = ("total_tokens", "total_tool_calls", "total_rounds")
    if any(type(metrics.get(k)) is not int or metrics[k] < 0 for k in names):
        raise ValueError("Published metrics must be nonnegative integers")
    return all(metrics[name] >= spec[limit] for name, limit in zip(
        names, ("min_tokens", "min_tool_calls", "min_assistant_steps")))


def selection_enabled(spec):
    value = spec.get("publish_selection", True)
    if type(value) is not bool:
        raise ValueError("publish_selection must be a boolean")
    return value


def origin_location(origin):
    offset = origin.get("source_byte_offset")
    if offset is not None and (type(offset) is not int or offset < 0):
        raise ValueError("Invalid raw source byte offset")
    return origin["source_ref"], -1 if offset is None else offset


def merge_origins(origins):
    """Keep each source once; combine its already-recorded quality evidence."""
    merged = {}
    severity = {None: -1, "pass": 0, "quarantined": 1, "excluded": 2}
    for origin in origins:
        item = dict(origin)
        key = origin_location(item)
        previous = merged.get(key)
        if previous is None:
            merged[key] = item
            continue
        if any(previous.get(k, "") != item.get(k, "") for k in ("sha256", "captured_at")):
            raise ValueError("Conflicting immutable lineage source")
        previous["reason_codes"] = sorted(set(previous.get("reason_codes", []))
                                          | set(item.get("reason_codes", [])))
        disposition = max((previous.get("disposition"), item.get("disposition")),
                          key=severity.__getitem__)
        if disposition is not None:
            previous["disposition"] = disposition
    return [merged[k] for k in sorted(merged)]


def normalized_manifest(source, files, counts, run):
    value = dict(source)
    value.update(files=sorted(files, key=lambda f: f["path"]), counts=counts,
                 created_at=run["created_at"], started_at_ms=run["started_at_ms"],
                 completed_at_ms=run["completed_at_ms"], config_hash=run["publication_id"],
                 status="SUCCEEDED", record_count=counts["accepted"] + counts["quarantined_trajectories"],
                 accepted_trajectory_count=counts["accepted"],
                 quarantined_trajectory_count=counts["quarantined_trajectories"],
                 total_bytes=sum(f["bytes"] for f in files if f["path"] != "lineage.jsonl"))
    return value


def selection_manifest(model, rows, spec):
    dates = [row["date"] for row in rows]
    return {
        "model": model, "source": spec["normalized_uri"].rstrip("/").rsplit("/", 1)[0] + "/",
        "min_tokens": spec["min_tokens"], "min_tool_calls": spec["min_tool_calls"],
        "min_assistant_steps": spec["min_assistant_steps"],
        "date_start": min(dates), "date_end": max(dates),
        "trajectory_count": len(rows), "total_bytes": sum(row["bytes"] for row in rows),
    }


_S3_UTC_TIMESTAMP = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z"
)
_MAX_CACHED_WITNESS_BYTES = 8 * 1024**2


def _parse_s3_timestamp(value):
    """Use the ISO parser for UTC object timestamps and the SDK for other wire formats."""
    if isinstance(value, str) and _S3_UTC_TIMESTAMP.fullmatch(value):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            pass
    from botocore.utils import parse_timestamp

    return parse_timestamp(value)


class PublicationStore:
    """S3 writes confined to explicit new versions, with run-owned retries."""

    def __init__(self, spec, publication_id, client=None, *, bulk_inventory=False):
        require_prefix(spec)
        self.spec, self.owner = spec, publication_id
        self._last_witness = None
        self.bucket, self.source = location(spec["source_uri"])
        bucket, self.normalized = location(spec["normalized_uri"])
        if bucket != self.bucket:
            raise ValueError("Publication must stay in one bucket")
        self.selections = {}
        if selection_enabled(spec):
            selection_bucket, base = location(spec["selection_base_uri"])
            if selection_bucket != self.bucket:
                raise ValueError("Publication must stay in one bucket")
            self.selections = {
                model: base + model_component(model) + "/" + relative_path(spec["selection_version"]) + "/"
                for model in spec["models"]
            }
        if len(set(self.selections.values())) != len(self.selections):
            raise ValueError("Model directory collision")
        prefixes = [self.source, self.normalized, *self.selections.values()]
        if any(a.startswith(b) or b.startswith(a)
               for i, a in enumerate(prefixes) for b in prefixes[i + 1:]):
            raise ValueError("Source and target prefixes must be disjoint")
        self.targets = [self.normalized, *self.selections.values()]
        self.manifest_keys = {p + "manifest.json" for p in self.targets}
        if client is None:
            import boto3
            from botocore.config import Config

            create_client = boto3.client
            if bulk_inventory:
                from botocore.session import get_session

                # A private Driver session keeps the parser local to this publication.
                session = get_session()
                session.get_component("response_parser_factory").set_parser_defaults(
                    timestamp_parser=_parse_s3_timestamp)
                create_client = boto3.Session(botocore_session=session).client
            client = create_client(
                "s3", endpoint_url=spec["s3_endpoint"], region_name=spec["s3_region"],
                config=Config(retries={"mode": "standard", "max_attempts": 8},
                              connect_timeout=20, read_timeout=180),
                **credentials(spec["s3_credentials_file"]))
        self.client = client
        if hasattr(client, "meta"):
            client.meta.events.register("before-parameter-build.s3.*", self.guard)

    def guard(self, model, params, **kwargs):
        key = params.get("Key", params.get("Prefix", ""))
        operation = model.name
        target = any(key.startswith(p) for p in self.targets)
        if params.get("Bucket") != self.bucket:
            raise ValueError("S3 operation outside publication bucket")
        if operation in {"GetObject", "HeadObject", "ListObjectsV2"}:
            allowed = key.startswith(self.source) or target
        elif operation == "CopyObject":
            origin = params.get("CopySource", {})
            allowed = (target and key.endswith(".jsonl") and isinstance(origin, dict)
                       and origin.get("Bucket") == self.bucket
                       and any(origin.get("Key", "").startswith(p)
                               for p in (self.source, self.normalized))
                       and bool(params.get("CopySourceIfMatch")))
        elif operation == "PutObject":
            allowed = key in self.manifest_keys or key == self.normalized + "lineage.jsonl"
        else:
            allowed = False
        if not allowed:
            raise ValueError("S3 operation outside publication scope: " + operation)

    def inventory_items(self, prefix, *, after=None):
        params = {"Bucket": self.bucket, "Prefix": prefix}
        if after is not None:
            params["StartAfter"] = prefix + after
        for page in self.client.get_paginator("list_objects_v2").paginate(**params):
            for entry in page.get("Contents", []):
                yield entry["Key"][len(prefix):], entry["Size"]

    def inventory(self, prefix):
        return dict(self.inventory_items(prefix))

    def head(self, key):
        try:
            return self.client.head_object(Bucket=self.bucket, Key=key)
        except Exception as error:
            code = str(getattr(error, "response", {}).get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return None
            if code in {"403", "AccessDenied"}:
                listed = self.client.list_objects_v2(Bucket=self.bucket, Prefix=key, MaxKeys=2)
                if not any(x["Key"] == key for x in listed.get("Contents", [])):
                    return None
            raise

    def read(self, key, expected=None, etag=None):
        response = self.client.get_object(Bucket=self.bucket, Key=key,
                                          **({"IfMatch": etag} if etag else {}))
        try:
            data = response["Body"].read()
        finally:
            response["Body"].close()
        if expected and (len(data) != expected["bytes"] or sha256(data) != expected["sha256"]):
            raise ValueError("Object content differs from frozen input: " + key)
        return data, response["ETag"]

    def read_witness(self, target):
        """Reuse the last verified representative's immutable bytes within one inspection task."""
        identity = (target["path"], target["bytes"], target["sha256"])
        if self._last_witness is not None and self._last_witness[0] == identity:
            return self._last_witness[1]
        self._last_witness = None
        data, _ = self.read(self.source + target["path"], target)
        if len(data) <= _MAX_CACHED_WITNESS_BYTES:
            self._last_witness = (identity, data)
        return data

    def verify(self, key, expected, etag=None):
        response = self.client.get_object(Bucket=self.bucket, Key=key,
                                          **({"IfMatch": etag} if etag else {}))
        digest, size = hashlib.sha256(), 0
        try:
            for chunk in response["Body"].iter_chunks(chunk_size=4 * 1024 * 1024):
                digest.update(chunk)
                size += len(chunk)
        finally:
            response["Body"].close()
        if size != expected["bytes"] or digest.hexdigest() != expected["sha256"]:
            raise ValueError("Published object hash mismatch: " + key)
        return response["ETag"]

    def download(self, key, path, expected):
        path = Path(path)
        temporary = path.with_suffix(path.suffix + ".partial")
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        digest, size = hashlib.sha256(), 0
        try:
            with temporary.open("wb") as stream:
                for chunk in response["Body"].iter_chunks(chunk_size=4 * 1024 * 1024):
                    digest.update(chunk)
                    size += len(chunk)
                    stream.write(chunk)
        finally:
            response["Body"].close()
        if size != expected["bytes"] or digest.hexdigest() != expected["sha256"]:
            raise ValueError("Downloaded object differs from frozen source")
        temporary.replace(path)

    def put_file(self, key, path):
        from .storage import file_sha256

        path = Path(path)
        expected = {"bytes": path.stat().st_size, "sha256": file_sha256(path)}
        etag = self.existing_owned(key, expected)
        if etag is None:
            with path.open("rb") as stream:
                response = self.client.put_object(
                    Bucket=self.bucket, Key=key, Body=stream, ContentLength=expected["bytes"],
                    ContentType="application/x-ndjson", IfNoneMatch="*",
                    Metadata={"sha256": expected["sha256"], "publication-id": self.owner})
            etag = response["ETag"]
        self.verify(key, expected, etag)
        return expected

    def existing_owned(self, key, expected):
        head = self.head(key)
        if head is None:
            return None
        metadata = head.get("Metadata", {})
        if (metadata.get("publication-id") != self.owner
                or metadata.get("sha256") != expected["sha256"]
                or head["ContentLength"] != expected["bytes"]):
            raise ValueError("Target collision outside this verified run: " + key)
        return head["ETag"]

    def copy(self, source_key, target_key, expected, source_etag):
        etag = self.existing_owned(target_key, expected)
        if etag is None:
            if expected["bytes"] > 5 * 1024**3:
                raise ValueError("Object exceeds validated single-copy limit")
            response = self.client.copy_object(
                Bucket=self.bucket, Key=target_key,
                CopySource={"Bucket": self.bucket, "Key": source_key},
                CopySourceIfMatch=source_etag, MetadataDirective="REPLACE",
                Metadata={"sha256": expected["sha256"], "publication-id": self.owner},
                ContentType="application/x-ndjson")
            etag = response["CopyObjectResult"]["ETag"]
        return self.verify(target_key, expected, etag)

    def put(self, key, data):
        expected = {"sha256": sha256(data), "bytes": len(data)}
        etag = self.existing_owned(key, expected)
        if etag is None:
            response = self.client.put_object(
                Bucket=self.bucket, Key=key, Body=data,
                ContentType="application/json" if key.endswith(".json") else "application/x-ndjson",
                Metadata={"sha256": expected["sha256"], "publication-id": self.owner},
                IfNoneMatch="*")
            etag = response["ETag"]
        self.verify(key, expected, etag)
        return expected


def _node_for_reviewed_token_estimate(node, ref, review):
    """Use the independently reproduced estimate only in the validator's copy."""
    stored, recomputed = review["stored_total_tokens"], review["recomputed_total_tokens"]
    if (ref["sha256"] != review["sha256"] or type(stored) is not int
            or type(recomputed) is not int or recomputed < 0 or stored < 0
            or node.total_tokens != stored or stored == recomputed):
        raise ValueError("Reviewed token estimate differs from the frozen source or stored count")
    return node.model_copy(update={"total_tokens": recomputed})


def inspect_partition(lines, spec, owner):
    require_prefix(spec)
    from trajfoundry.canonical import trajectory_id
    from trajfoundry.export import trajectory_filename
    from trajfoundry.output_contract import parse_trajectory_record
    from trajfoundry.quality import is_strict_sample, validate_derived_fields

    store = PublicationStore(spec, owner)
    policy = Policy(**spec["policy"])
    reviews = spec.get("reviewed_token_estimates", {})
    for line in lines:
        decision = json.loads(line)
        ref = decision["record"]
        raw, etag = store.read(store.source + ref["path"], ref)
        try:
            value = json.loads(raw)
            node = parse_trajectory_record(value)
            identifier = trajectory_id(node)
            if trajectory_filename(node, identifier) != ref["path"]:
                raise ValueError("Filename mismatch")
            if decision["status"] == "kept":
                validation_node = node
                if ref["path"] in reviews:
                    validation_node = _node_for_reviewed_token_estimate(
                        node, ref, reviews[ref["path"]])
                validate_derived_fields(validation_node)
            strict = is_strict_sample(node)
            metrics = {k: value[k] for k in ("total_tokens", "total_tool_calls", "total_rounds")}
            qualifies = selected(metrics, spec) if selection_enabled(spec) else False
            if "models" in spec and node.model not in spec["models"]:
                raise ValueError("Unexpected model")
            if decision["status"] == "removed":
                target = decision["retained"]
                if not raw_covers(value, json.loads(store.read_witness(target)), ref, target, policy):
                    raise ValueError("Removal failed independent prefix verification")
        except Exception as error:
            raise ValueError("Trajectory validation failed: " + ref["path"] + " ("
                             + type(error).__name__ + ")") from None
        row = {
            "path": ref["path"], "bytes": ref["bytes"], "sha256": ref["sha256"],
            "etag": etag, "trajectory_id": identifier, "status": decision["status"],
            "final_path": decision.get("retained", ref)["path"],
            "model": node.model, "date": spec["date"], "metrics": metrics,
            "selected": qualifies, "strict": strict,
            "reason_codes": [] if strict else (value.get("normalization_audit") or {}).get("reason_codes", []),
        }
        if spec.get("archive_rows"):
            metadata = value["metadata"]["source_metadata"]
            row["archive"] = {
                "messages": len(value["messages"]),
                "success": metadata["original_record_fields"].get("success"),
                "rewrites": metadata["rewrites"],
            }
        yield row


def copy_partition(lines, spec, owner, selection=False):
    store = PublicationStore(spec, owner)
    for line in lines:
        row = json.loads(line)
        source_key = (store.normalized if selection else store.source) + row["path"]
        target_key = ((store.selections[row["model"]] + "dt=" + row["date"] + "/")
                      if selection else store.normalized) + row["path"]
        source_etag = row["normalized_etag"] if selection else row["etag"]
        etag = store.copy(source_key, target_key, row, source_etag)
        yield {**row, "selected_etag" if selection else "normalized_etag": etag}


def verify_copies_partition(lines, spec, owner, selection=False):
    store = PublicationStore(spec, owner)
    count = 0
    for row in lines:
        key = ((store.selections[row["model"]] + "dt=" + row["date"] + "/")
               if selection else store.normalized) + row["path"]
        etag = store.existing_owned(key, row)
        if etag is None:
            raise ValueError("A completed copy checkpoint lost an output object")
        store.verify(key, row, etag)
        count += 1
    yield count


def merge_lineage_group(item):
    from trajfoundry.validation import _LineageRecord

    target, items = item
    records = list(items)
    representative = [lineage for row, lineage in records if row["path"] == target]
    if len(representative) != 1:
        raise ValueError("Lineage has no unique retained representative")
    result = dict(representative[0])
    result["origins"] = merge_origins(o for _, record in records for o in record["origins"])
    result["origin_count"] = len(result["origins"])
    _LineageRecord.model_validate(result)
    return target, result


def reason_counts(rows):
    counter = Counter()
    for row in rows:
        counter.update(row["reason_codes"])
    return counter


def write_json(path, value):
    from .storage import atomic_bytes
    data = encoded(value)
    atomic_bytes(Path(path), data, sha256(data))
