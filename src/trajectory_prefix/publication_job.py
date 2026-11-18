"""Publish prefix decisions on the cluster and verify each removal against its final representative."""

from __future__ import annotations

import hashlib
import json
import tempfile
import time
import uuid
import zipfile
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path

from botocore.exceptions import ClientError

from .archive_publication import published_archive, verify_archive_sources
from .comparison import prefix_source_report, require_prefix
from .publication import (
    PublicationStore,
    encoded,
    merge_lineage_group,
    normalized_manifest,
    origin_location,
    selection_enabled,
    selection_manifest,
    sha256,
    write_json,
)
from .storage import file_sha256, reference, relative_path


def emit(event, **values):
    print(json.dumps({"event": event, **values}, ensure_ascii=False), flush=True)


def verify_retained_plan(recheck, decisions, remaining):
    digest = hashlib.sha256()
    with Path(recheck["input_plan"]).open("rb") as stream:
        for line in stream:
            digest.update(line)
            if not line.strip():
                continue
            ref = reference(json.loads(line))
            path = ref["path"]
            if path not in remaining:
                raise ValueError("Duplicate or unexpected file in retained recheck plan")
            expected = decisions[path]["record"]
            if any(ref[key] != expected[key] for key in ("id", "bytes", "sha256")):
                raise ValueError("Retained recheck file differs from the kept decision")
            remaining.remove(path)
    if remaining or digest.hexdigest() != recheck["input_plan_sha256"]:
        raise ValueError("Incomplete or changed retained recheck plan")


def verify_audit_report(spec, run, report):
    audit_path = Path(spec["audit_report"])
    audit = json.loads(audit_path.read_bytes())
    require_prefix(audit)
    if not ((audit_path.parent / "SUCCEEDED").is_file()
            and audit.get("passed") is True and Path(audit["source_run"]).resolve() == run.resolve()
            and audit["policy"] == report["policy"]
            and audit["input_plan_sha256"] == report["input_plan_sha256"]):
        raise ValueError("Completed independent prefix audit must match the source run, policy and input plan")
    if spec["recheck_report"] is None:
        counts = audit["counts"].get("all", {})
        retained_metric = "retained_input_hash_verified" if report.get("check_only") else "output_hash_verified"
        expected_mode = "input_decisions" if report.get("check_only") else "published_files"
        expected = {
            "input": report["input_count"],
            "kept": report["kept_count"],
            "removed": report["removed_count"],
            retained_metric: report["kept_count"],
            "witness_verified": report["removed_count"],
        }
        if audit.get("audit_mode") != expected_mode or any(
            counts.get(key, 0) != value for key, value in expected.items()
        ):
            raise ValueError("Independent prefix audit does not cover every input, retained hash and removal witness")


def read_decisions(spec, source, disk_path=None):
    report = prefix_source_report(spec)
    run = Path(spec["source_run"])
    if not ((run / "SUCCEEDED").is_file() and (run / "decisions/_SUCCESS").is_file()
            and report["policy"] == spec["policy"]):
        raise ValueError("Completed matching decisions are required")
    if (report["input_plan_sha256"] != spec["input_plan_sha256"]
            or file_sha256(Path(report["input_plan"])) != spec["input_plan_sha256"]):
        raise ValueError("Source run differs from the frozen input plan")
    if spec["audit_report"] is not None:
        verify_audit_report(spec, run, report)
    recheck = None
    if spec["recheck_report"] is not None:
        recheck_path = Path(spec["recheck_report"])
        recheck = json.loads(recheck_path.read_bytes())
        require_prefix(recheck)
        if not ((recheck_path.parent / "SUCCEEDED").is_file() and recheck["policy"] == report["policy"]
                and recheck["removed_count"] == 0
                and recheck["input_count"] == recheck["kept_count"] == report["kept_count"]):
            raise ValueError("Retained recheck must be complete and match the first-pass results")
    remaining_files = {
        entry["path"]: entry for entry in source["files"] if entry["path"] != "lineage.jsonl"
    }
    from .disk_records import DiskRecords

    decisions = DiskRecords(disk_path) if disk_path else {}
    inventory = {}
    kept_count = 0
    emit("decision_index_started", input_count=report["input_count"])
    for path in sorted((run / "decisions").glob("part-*")):
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for line in stream:
                digest.update(line)
                row = json.loads(line)
                ref = row["record"]
                # Consuming the manifest entry also detects a repeated decision path.
                manifest_entry = remaining_files.pop(ref["path"], None)
                if (row["status"] not in {"kept", "removed"}
                        or ref["dataset"] != spec["dataset"] or ref["date"] != spec["date"]
                        or ref["id"] != "/".join((ref["dataset"], ref["date"], ref["path"]))
                        or ref["uri"] != spec["source_uri"] + ref["path"]
                        or (row["status"] == "kept" and "retained" in row)
                        or manifest_entry != {k: ref[k] for k in ("path", "bytes", "sha256")}):
                    raise ValueError("Decisions differ from the frozen manifest")
                if row["status"] == "kept":
                    kept_count += 1
                if disk_path:
                    decisions.add(ref["path"], row, ref["id"])
                else:
                    decisions[ref["path"]] = row
        inventory[path.name] = digest.hexdigest()
        if disk_path:
            decisions.commit()
        if len(inventory) % 256 == 0:
            emit("decision_index_progress", files=len(inventory), rows=len(decisions))
    if sha256(encoded(inventory)) != spec["decision_files_sha256"]:
        raise ValueError("Frozen decisions changed")
    input_count = len(decisions)
    if remaining_files or input_count != report["input_count"] or kept_count != report["kept_count"]:
        raise ValueError("Decision accounting mismatch")
    del remaining_files
    if input_count - kept_count != report["removed_count"]:
        raise ValueError("Removal count differs from the first-pass report")
    removed_rows = decisions.view("removed") if disk_path else (
        row for row in decisions.values() if row["status"] == "removed")
    for row in removed_rows:
        retained = row["retained"]
        target = decisions.get(retained["path"])
        if target is None or target["status"] != "kept" or target["record"] != retained:
            raise ValueError("Removal does not name a final retained input")
    if recheck is not None:
        kept = {row["record"]["path"] for row in decisions.values() if row["status"] == "kept"}
        verify_retained_plan(recheck, decisions, kept)
    emit("decision_evidence_verified", input_count=input_count, kept_count=kept_count)
    return decisions, report


def verify_inspected_decisions(by_path, decisions):
    if by_path.keys() != decisions.keys():
        raise ValueError("Inspection does not cover all decisions")
    verified_removed = 0
    for path, row in by_path.items():
        decision = decisions[path]
        if (row["status"] != decision["status"]
                or row["final_path"] != decision.get("retained", decision["record"])["path"]
                or any(row[k] != decision["record"][k] for k in ("sha256", "bytes"))):
            raise ValueError("Inspected row differs from frozen decision")
        if row["status"] == "removed":
            verified_removed += 1
    return verified_removed


def publication_counts(rows, source_counts, duplicates):
    input_reasons = Counter()
    removed_reasons = Counter()
    retained_models = Counter()
    accepted = quarantined = selected_count = 0
    for row in rows:
        reasons = row["reason_codes"]
        input_reasons.update(reasons)
        if row["status"] != "kept":
            removed_reasons.update(reasons)
            continue
        retained_models[row["model"]] += 1
        accepted += row["strict"]
        quarantined += not row["strict"]
        selected_count += row["selected"]

    declared = source_counts["reason_counts"]
    if any(declared.get(key, 0) < count for key, count in input_reasons.items()):
        raise ValueError("Source reason accounting cannot be separated")
    # Source counts include raw failures. Subtract only reasons on removed trajectories.
    counts = {**source_counts, "accepted": accepted, "quarantined_trajectories": quarantined,
              "duplicate_trajectories": duplicates,
              "reason_counts": dict(sorted((Counter(declared) - removed_reasons).items()))}
    return counts, dict(sorted(retained_models.items())), selected_count


def stage_receipt(output, name, version=None):
    original = output / "stages" / (name + ".json")
    if version and original.is_file() and json.loads(original.read_bytes()).get("version") != version:
        return original.with_name(f"{name}-{version}.json")
    return original


def stage(sc, output, name, owner, make, *, version=None):
    receipt = stage_receipt(output, name, version)
    if receipt.exists():
        value = json.loads(receipt.read_text())
        if (value.get("version") != version or value["publication_id"] != owner
                or not (Path(value["path"]) / "_SUCCESS").is_file()):
            raise ValueError("Invalid publication checkpoint")
        emit("reuse_stage", stage=name, rows=value["rows"])
    else:
        target = output / (name + "-" + uuid.uuid4().hex[:12])
        emit("stage_started", stage=name)
        make().map(lambda row: encoded(row).decode().rstrip("\n")).saveAsTextFile(str(target))
        count = sc.textFile(str(target)).filter(bool).count()
        value = {"publication_id": owner, "path": str(target), "rows": count}
        if version is not None:
            value["version"] = version
        write_json(receipt, value)
        emit("stage_completed", stage=name, rows=count)
    return sc.textFile(value["path"]).filter(bool).map(json.loads)


_INVENTORY_KEYS_PER_RANGE = 40_000


class _UnorderedListing(Exception):
    """The endpoint did not honor ordered ListObjectsV2 ranges."""


def require_inventory(store, prefix, expected, *, allow_manifest=True):
    mismatch = "File collection or byte counts differ: " + prefix
    workers = min(8, (len(expected) + _INVENTORY_KEYS_PER_RANGE - 1) // _INVENTORY_KEYS_PER_RANGE)
    if workers > 1 and hasattr(store, "inventory_items"):
        started = time.monotonic()
        names = sorted(expected)
        boundaries = [names[len(names) * i // workers - 1] for i in range(1, workers)]
        del names

        def check_range(bounds):
            after, through = bounds
            previous = after
            matched = 0
            for name, size in store.inventory_items(prefix, after=after):
                if previous is not None and name <= previous:
                    raise _UnorderedListing
                if through is not None and name > through:
                    break
                if not (allow_manifest and name == "manifest.json"):
                    if name not in expected or expected[name] != size:
                        raise ValueError(mismatch)
                    matched += 1
                if name == through:
                    break
                previous = name
            return matched

        # Disjoint (after, through] ranges also cover keys before the first
        # expected object and beyond the last one, including unexpected files.
        try:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                total = sum(pool.map(check_range, pairwise([None, *boundaries, None])))
        except _UnorderedListing:
            pass
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") not in {
                "InvalidArgument", "InvalidRequest", "NotImplemented", "NotSupported"
            }:
                raise
        else:
            if total == len(expected):
                emit("inventory_ranges_verified", objects=total, ranges=workers,
                     elapsed_seconds=round(time.monotonic() - started, 2))
                return

    # Reconcile a short or unordered range listing with the original full
    # inventory. Small partitions also use this path.
    actual = store.inventory(prefix)
    if allow_manifest:
        actual.pop("manifest.json", None)
    if actual != expected:
        raise ValueError(mismatch)


def source_inventory(spec, source, manifest_bytes):
    """Include only ancillary objects recorded by the frozen full-prefix scan."""
    receipt_path = Path(spec["source_inventory_receipt"])
    if file_sha256(receipt_path) != spec["source_inventory_sha256"]:
        raise ValueError("Frozen source inventory receipt changed")
    receipt = json.loads(receipt_path.read_text())
    files = {entry["path"]: entry["bytes"] for entry in source["files"]}
    trajectory_files = {k: v for k, v in files.items() if k != "lineage.jsonl"}
    if (receipt["source_uri"] != spec["source_uri"]
            or receipt["source_manifest_sha256"] != spec["source_manifest_sha256"]
            or receipt["input_plan_sha256"] != spec["input_plan_sha256"]
            or receipt.get("complete_requested_prefix") is not True
            or receipt["input_count"] != len(trajectory_files)
            or receipt["input_bytes"] != sum(trajectory_files.values())):
        raise ValueError("Frozen source inventory does not match publication inputs")
    files["manifest.json"] = manifest_bytes
    seen = set()
    for entry in receipt["non_trajectory_files"]:
        path, size = relative_path(entry["path"]), entry["bytes"]
        if (path in seen or path in trajectory_files or type(size) is not int or size < 0
                or (path in files and files[path] != size)):
            raise ValueError("Invalid ancillary source inventory")
        seen.add(path)
        files[path] = size
    return files


def run(spec_path, output, partitions=512):
    spec = json.loads(Path(spec_path).read_bytes())
    prefix_source_report(spec)

    import trajfoundry
    from pyspark.sql import SparkSession
    from trajfoundry.validation import _LineageRecord, _Manifest

    import trajectory_prefix

    owner = sha256(encoded(spec))
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    (output / "stages").mkdir(exist_ok=True)
    run_path = output / "run.json"
    store = PublicationStore(spec, owner)
    if run_path.exists():
        state = json.loads(run_path.read_text())
        if state["publication_id"] != owner:
            raise ValueError("Shared output belongs to a different publication")
    else:
        for target in store.targets:
            if store.inventory(target):
                raise ValueError("New publication requires empty target versions")
        state = {"publication_id": owner, "created_at": datetime.now(UTC).isoformat(),
                 "started_at_ms": int(time.time() * 1000)}
        write_json(run_path, state)
    spark = SparkSession.builder.appName("trajectory-prefix-publish").getOrCreate()
    sc = spark.sparkContext
    if not sc.master.startswith("k8s://"):
        spark.stop()
        raise ValueError("Business publication requires Spark Cluster")
    sc.setLogLevel("WARN")
    # Indexes are reconstructible from frozen shared evidence; random SQLite
    # lookups belong on the Driver's local scratch volume, not on shared NFS.
    scratch = tempfile.TemporaryDirectory(prefix="prefix-publication-index-")
    index_root = Path(scratch.name)
    package = output / ("publication-package-" + uuid.uuid4().hex + ".zip")
    with zipfile.ZipFile(package, "w", zipfile.ZIP_DEFLATED) as archive:
        for module in (trajectory_prefix, trajfoundry):
            root = Path(module.__file__).resolve().parent
            for file in root.rglob("*.py"):
                archive.write(file, module.__name__ + "/" + str(file.relative_to(root)))
    sc.addPyFile(str(package))
    try:
        source_manifest_key = store.source + "manifest.json"
        raw_manifest, manifest_etag = store.read(source_manifest_key)
        manifest_identity = {"bytes": len(raw_manifest), "sha256": spec["source_manifest_sha256"]}
        if sha256(raw_manifest) != manifest_identity["sha256"]:
            raise ValueError("Source manifest changed")
        source = json.loads(raw_manifest)
        del raw_manifest
        _Manifest.model_validate(source)
        if bool(spec.get("archive_rows")) != (source.get("sessionless_partitioning") == "archive-record-v1"):
            raise ValueError("Archive row accounting must match the source manifest")
        write_json(output / "source-manifest.json", source)
        emit("source_manifest_verified", files=len(source["files"]))
        expected_inventory = source_inventory(spec, source, manifest_identity["bytes"])
        require_inventory(store, store.source, expected_inventory, allow_manifest=False)
        emit("source_inventory_verified", files=len(expected_inventory))
        del expected_inventory
        from .disk_records import DiskRecords

        normalized_only = not selection_enabled(spec)
        decisions, prior_report = read_decisions(
            spec, source, index_root / "decisions.sqlite" if normalized_only else None)
        # The frozen decision index now covers every source file. Only the lineage entry
        # is needed from the multi-million-row source file list after this point.
        lineage_entry = next(x for x in source.pop("files") if x["path"] == "lineage.jsonl")
        emit("source_verified", input_count=len(decisions), kept_count=prior_report["kept_count"],
             normalized_uri=spec["normalized_uri"], publish_selection=not normalized_only,
             selection_version=spec.get("selection_version"))

        from .publication import copy_partition, inspect_partition, verify_copies_partition
        inputs = sc.textFile(str(Path(spec["source_run"]) / "decisions"), minPartitions=partitions).filter(bool)
        inspection_version = "prefix-witness-v1"
        inspected = stage(sc, output, "source-inspection", owner,
                          lambda: inputs.mapPartitions(lambda rows: inspect_partition(rows, spec, owner)),
                          version=inspection_version)
        if normalized_only:
            inspected_path = json.loads(stage_receipt(output, "source-inspection", inspection_version).read_bytes())["path"]
            inspected_records = DiskRecords.from_parts(
                index_root / "inspected.sqlite", inspected_path)
            rows = inspected_records.view()
        else:
            rows = inspected.collect()
        input_count = len(rows)
        if input_count != len(decisions) or len({r["trajectory_id"] for r in rows}) != input_count:
            raise ValueError("Source trajectory IDs or count are not unique")
        by_path = inspected_records if normalized_only else {row["path"]: row for row in rows}
        verified_removed = verify_inspected_decisions(by_path, decisions)
        kept = inspected_records.view("kept") if normalized_only else [row for row in rows if row["status"] == "kept"]
        kept_count = len(kept)
        if kept_count != prior_report["kept_count"] or verified_removed != prior_report["removed_count"]:
            raise ValueError("Inspection counts differ from the first-pass report")

        # Freeze lineage bytes before joins; keep the source contract and raw provenance.
        lineage_path = output / "source-lineage.jsonl"
        if not lineage_path.exists():
            store.download(store.source + "lineage.jsonl", lineage_path, lineage_entry)
        if file_sha256(lineage_path) != lineage_entry["sha256"]:
            raise ValueError("Frozen lineage changed")
        lineage = sc.textFile(str(lineage_path), minPartitions=partitions).filter(bool).map(json.loads)
        lineage_ids = lineage.map(lambda row: row["trajectory_id"])
        if lineage.count() != input_count or lineage_ids.distinct().count() != input_count:
            raise ValueError("Source lineage IDs are not unique")
        by_identifier = inspected.keyBy(lambda row: row["trajectory_id"])
        joined = by_identifier.join(lineage.keyBy(lambda row: row["trajectory_id"]), partitions)
        if joined.count() != input_count:
            raise ValueError("Source lineage does not cover source trajectories")
        merged = stage(sc, output, "merged-lineage", owner,
                       lambda: joined.values().map(lambda pair: (pair[0]["final_path"], pair))
                       .groupByKey(partitions).map(merge_lineage_group)
                       .map(lambda pair: {"path": pair[0], "lineage": pair[1]}))
        if merged.count() != kept_count:
            raise ValueError("Merged lineage count differs from retained count")
        original_origins = lineage.flatMap(lambda row: ((*origin_location(x), x["sha256"])
                                                       for x in row["origins"])).distinct()
        merged_origins = merged.flatMap(lambda row: ((*origin_location(x), x["sha256"])
                                                     for x in row["lineage"]["origins"])).distinct()
        if original_origins.subtract(merged_origins).take(1) or merged_origins.subtract(original_origins).take(1):
            raise ValueError("Raw source coverage changed during lineage merge")
        origin_count = original_origins.count()
        if spec.get("archive_rows"):
            verify_archive_sources(source["archive"], original_origins, origin_count, source["counts"]["input_files"])
        elif origin_count + source["counts"]["skipped_inputs"] + source["counts"]["quarantined_records"] != source["counts"]["input_files"]:
            raise ValueError("Raw source accounting does not reconcile")
        merged_file = output / "lineage.jsonl"
        if not merged_file.exists():
            sorted_lineage = stage(sc, output, "sorted-lineage", owner,
                                   lambda: merged.sortBy(lambda r: r["path"], numPartitions=partitions))
            if sorted_lineage.count() != kept_count:
                raise ValueError("Sorted lineage count differs from retained count")
            sorted_path = Path(json.loads((output / "stages/sorted-lineage.json").read_text())["path"])
            temp = merged_file.with_suffix(".jsonl." + uuid.uuid4().hex + ".tmp")
            with temp.open("wb") as stream:
                for part in sorted(sorted_path.glob("part-*")):
                    with part.open() as records:
                        for line in records:
                            row = json.loads(line)
                            value = row["lineage"]
                            _LineageRecord.model_validate(value)
                            if value["trajectory_id"] != by_path[row["path"]]["trajectory_id"]:
                                raise ValueError("Merged lineage representative ID changed")
                            stream.write(encoded(value))
            temp.replace(merged_file)
        merged_entry = {"path": "lineage.jsonl", "bytes": merged_file.stat().st_size,
                        "sha256": file_sha256(merged_file)}
        duplicates = merged.map(lambda row: row["lineage"]["origin_count"] - 1).sum()
        counts, retained_models, selected_count = publication_counts(rows, source["counts"], duplicates)
        if spec.get("archive_rows"):
            source["archive"] = published_archive(source["archive"], rows, kept)
        manifest_path = output / "normalized-manifest.json"
        if manifest_path.exists():
            candidate = json.loads(manifest_path.read_text())
        else:
            run_state = {**state, "completed_at_ms": int(time.time() * 1000)}
            files = [{k: row[k] for k in ("path", "bytes", "sha256")} for row in kept]
            files.append(merged_entry)
            candidate = normalized_manifest(source, files, counts, run_state)
            del files
        _Manifest.model_validate(candidate)
        if candidate["counts"] != counts or candidate["config_hash"] != owner:
            raise ValueError("Manifest checkpoint no longer matches inspected inputs")
        emit("publication_preflight_passed", retained=kept_count, selected=selected_count,
             accepted=counts["accepted"], quarantined=counts["quarantined_trajectories"],
             unique_raw_sources=origin_count)

        kept_input = inspected.filter(lambda r: r["status"] == "kept").map(lambda r: encoded(r).decode())
        resumed_normalized = (output / "stages/normalized-copy.json").exists()
        copies = stage(sc, output, "normalized-copy", owner,
                       lambda: kept_input.mapPartitions(lambda it: copy_partition(it, spec, owner)))
        if resumed_normalized:
            if copies.mapPartitions(lambda it: verify_copies_partition(it, spec, owner)).sum() != kept_count:
                raise ValueError("Resumed normalized outputs failed verification")
        if normalized_only:
            copy_path = json.loads((output / "stages/normalized-copy.json").read_text())["path"]
            normalized_count = inspected_records.verify_retained_copies(copy_path)
        else:
            copy_rows = copies.collect()
            normalized_count = len(copy_rows)
            if normalized_count != kept_count or {r["path"] for r in copy_rows} != {r["path"] for r in kept}:
                raise ValueError("Normalized copy receipts are incomplete")
        store.put_file(store.normalized + "lineage.jsonl", merged_file)
        require_inventory(store, store.normalized, {f["path"]: f["bytes"] for f in candidate["files"]})
        # A source manifest change blocks publication even if all copied objects matched.
        store.verify(source_manifest_key, manifest_identity, manifest_etag)
        if not manifest_path.exists():
            candidate["completed_at_ms"] = int(time.time() * 1000)
            write_json(manifest_path, candidate)
        store.put(store.normalized + "manifest.json", encoded(candidate))
        emit("normalized_published", count=normalized_count, total_bytes=candidate["total_bytes"])

        selected_rows, model_reports = [], []
        if not normalized_only:
            eligible = [row for row in copy_rows if row["selected"]]
            selected_input = sc.parallelize([encoded(row).decode() for row in eligible], partitions)
            resumed_selection = (output / "stages/selection-copy.json").exists()
            selected_copies = stage(sc, output, "selection-copy", owner,
                                   lambda: selected_input.mapPartitions(
                                       lambda it: copy_partition(it, spec, owner, selection=True)))
            if resumed_selection:
                if selected_copies.mapPartitions(
                        lambda it: verify_copies_partition(it, spec, owner, selection=True)).sum() != len(eligible):
                    raise ValueError("Resumed selected outputs failed verification")
            selected_rows = selected_copies.collect()
            if (len(selected_rows) != len(eligible)
                    or {r["path"] for r in selected_rows} != {r["path"] for r in eligible}):
                raise ValueError("Selection copy receipts are incomplete")
            groups = defaultdict(list)
            for row in selected_rows:
                groups[row["model"]].append(row)
            for model in spec["models"]:
                group = groups.get(model, [])
                prefix = store.selections[model]
                require_inventory(store, prefix, {"dt=" + r["date"] + "/" + r["path"]: r["bytes"] for r in group})
                if not group:
                    continue
                short_manifest = selection_manifest(model, group, spec)
                write_json(output / "model-manifests" / (model.replace("/", "_") + ".json"), short_manifest)
                store.put(prefix + "manifest.json", encoded(short_manifest))
                model_reports.append({**short_manifest, "output_uri": "s3://" + store.bucket + "/" + prefix})
        store.verify(source_manifest_key, manifest_identity, manifest_etag)
        report = {
            "passed": True, "publication_id": owner, "spark_master": sc.master,
            "spark_version": spark.version, "source_uri": spec["source_uri"],
            "normalized_uri": spec["normalized_uri"], "input_count": input_count,
            "kept_count": kept_count, "removed_count": input_count - kept_count,
            "selected_count": len(selected_rows), "normalized_bytes": candidate["total_bytes"],
            "selected_bytes": sum(r["bytes"] for r in selected_rows),
            "accepted": counts["accepted"], "quarantined": counts["quarantined_trajectories"],
            "policy": spec["policy"], "models": model_reports,
            "publish_selection": selection_enabled(spec),
            "comparison_mode": spec.get("comparison_mode", "prefix"),
            "retained_models": retained_models,
            "verification": {"independent_audit_performed": spec["audit_report"] is not None,
                             "retained_recheck_performed": spec["recheck_report"] is not None,
                             "removal_witnesses_verified": verified_removed,
                             "source_hashes": input_count, "normalized_target_hashes": normalized_count,
                             "selected_target_hashes": len(selected_rows),
                             "derived_fields_checked": kept_count, "lineage_rows": kept_count,
                             "unique_raw_sources": origin_count, "source_manifest_unchanged": True},
        }
        write_json(output / "report.json", report)
        (output / "SUCCEEDED").write_text("All target bytes, inventories, metrics and lineage checked.\n")
        emit("publication_completed", kept=kept_count, selected=len(selected_rows), models=len(model_reports))
        return report
    finally:
        for name in ("decisions", "inspected_records"):
            collection = locals().get(name)
            if hasattr(collection, "db"):
                collection.db.close()
        scratch.cleanup()
        spark.stop()
