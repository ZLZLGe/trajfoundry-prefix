"""Cluster-only publication of an independently audited prefix result."""

from __future__ import annotations

import json
import time
import uuid
import zipfile
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path

from .publication import (
    PublicationStore,
    encoded,
    merge_lineage_group,
    normalized_manifest,
    reason_counts,
    selection_enabled,
    selection_manifest,
    sha256,
    write_json,
)
from .storage import file_sha256, relative_path


def emit(event, **values):
    print(json.dumps({"event": event, **values}, ensure_ascii=False), flush=True)


def read_decisions(spec, source, disk_path=None):
    run = Path(spec["source_run"])
    report = json.loads((run / "report.json").read_text())
    if not ((run / "SUCCEEDED").is_file() and (run / "decisions/_SUCCESS").is_file()
            and report["policy"] == spec["policy"]):
        raise ValueError("Completed matching decisions are required")
    mode = spec.get("comparison_mode", "prefix")
    if mode == "prefix":
        audit = json.loads(Path(spec["audit_report"]).read_text())
        recheck = json.loads(Path(spec["recheck_report"]).read_text())
        if not (audit.get("passed") and audit["policy"] == report["policy"]
                and recheck["policy"] == spec["policy"] and recheck["removed_count"] == 0
                and recheck["input_count"] == report["kept_count"]
                and audit["input_plan_sha256"] == report["input_plan_sha256"]
                == spec["input_plan_sha256"]
                and file_sha256(Path(report["input_plan"])) == spec["input_plan_sha256"]):
            raise ValueError("Completed independent audit and recheck are required")
    elif mode == "placeholder-relaxed-v1":
        from .publication_evidence import validate_mask_report

        validate_mask_report(spec, report)
    else:
        raise ValueError("Unsupported publication comparison mode")
    files = {entry["path"]: entry for entry in source["files"] if entry["path"] != "lineage.jsonl"}
    from .disk_records import DiskRecords

    decisions = DiskRecords(disk_path) if disk_path else {}
    inventory = {}
    for path in sorted((run / "decisions").glob("part-*")):
        inventory[path.name] = file_sha256(path)
        with path.open() as stream:
            for line in stream:
                row = json.loads(line)
                ref = row["record"]
                if (ref["path"] in decisions or row["status"] not in {"kept", "removed"}
                        or ref["dataset"] != spec["dataset"] or ref["date"] != spec["date"]
                        or ref["uri"] != spec["source_uri"] + ref["path"]
                        or files.get(ref["path"]) != {k: ref[k] for k in ("path", "bytes", "sha256")}):
                    raise ValueError("Decisions differ from the frozen manifest")
                if disk_path:
                    decisions.add(ref["path"], row, ref["id"])
                else:
                    decisions[ref["path"]] = row
        if disk_path:
            decisions.commit()
    if sha256(encoded(inventory)) != spec["decision_files_sha256"]:
        raise ValueError("Frozen decisions changed")
    kept = {k for k, v in decisions.items() if v["status"] == "kept"}
    if (decisions.keys() != files.keys() or len(decisions) != report["input_count"]
            or len(kept) != report["kept_count"]):
        raise ValueError("Decision accounting mismatch")
    for row in decisions.values():
        if row["status"] == "removed":
            retained = row["retained"]
            if (retained["path"] not in kept
                    or retained != decisions[retained["path"]]["record"]):
                raise ValueError("Removal does not name a final retained input")
    if len(decisions) - len(kept) != report["removed_count"]:
        raise ValueError("Removal count differs from audited count")
    if mode == "placeholder-relaxed-v1":
        from .publication_evidence import validate_mask_decisions, validate_mask_decisions_streamed

        if disk_path:
            validate_mask_decisions_streamed(spec, report, decisions)
        else:
            validate_mask_decisions(spec, report, decisions)
    return decisions, report


def stage(sc, output, name, owner, make):
    receipt = output / "stages" / (name + ".json")
    if receipt.exists():
        value = json.loads(receipt.read_text())
        if value["publication_id"] != owner or not (Path(value["path"]) / "_SUCCESS").is_file():
            raise ValueError("Invalid publication checkpoint")
        emit("reuse_stage", stage=name, rows=value["rows"])
    else:
        target = output / (name + "-" + uuid.uuid4().hex[:12])
        emit("stage_started", stage=name)
        make().map(lambda row: encoded(row).decode().rstrip("\n")).saveAsTextFile(str(target))
        count = sc.textFile(str(target)).filter(bool).count()
        value = {"publication_id": owner, "path": str(target), "rows": count}
        write_json(receipt, value)
        emit("stage_completed", stage=name, rows=count)
    return sc.textFile(value["path"]).filter(bool).map(json.loads)


def require_inventory(store, prefix, expected, *, allow_manifest=True):
    actual = store.inventory(prefix)
    if allow_manifest:
        actual.pop("manifest.json", None)
    if actual != expected:
        raise ValueError("File collection or byte counts differ: " + prefix)


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
    import trajfoundry
    from pyspark.sql import SparkSession
    from trajfoundry.validation import _LineageRecord, _Manifest

    import trajectory_prefix

    spec = json.loads(Path(spec_path).read_text())
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
    package = output / ("publication-package-" + uuid.uuid4().hex + ".zip")
    with zipfile.ZipFile(package, "w", zipfile.ZIP_DEFLATED) as archive:
        for module in (trajectory_prefix, trajfoundry):
            root = Path(module.__file__).resolve().parent
            for file in root.rglob("*.py"):
                archive.write(file, module.__name__ + "/" + str(file.relative_to(root)))
    sc.addPyFile(str(package))
    try:
        raw_manifest, manifest_etag = store.read(store.source + "manifest.json")
        if sha256(raw_manifest) != spec["source_manifest_sha256"]:
            raise ValueError("Source manifest changed")
        source = json.loads(raw_manifest)
        _Manifest.model_validate(source)
        write_json(output / "source-manifest.json", source)
        expected_inventory = source_inventory(spec, source, len(raw_manifest))
        require_inventory(store, store.source, expected_inventory, allow_manifest=False)
        from .disk_records import DiskRecords

        large = not selection_enabled(spec)
        decisions, prior_report = read_decisions(
            spec, source, output / ("decisions-" + uuid.uuid4().hex + ".sqlite") if large else None)
        emit("source_verified", input_count=len(decisions), kept_count=prior_report["kept_count"],
             normalized_uri=spec["normalized_uri"], publish_selection=selection_enabled(spec),
             selection_version=spec.get("selection_version"))

        from .publication import copy_partition, inspect_partition, verify_copies_partition
        inputs = sc.textFile(str(Path(spec["source_run"]) / "decisions"), minPartitions=partitions).filter(bool)
        inspected = stage(sc, output, "source-inspection", owner,
                          lambda: inputs.mapPartitions(lambda rows: inspect_partition(rows, spec, owner)))
        if large:
            inspected_path = json.loads((output / "stages/source-inspection.json").read_text())["path"]
            inspected_records = DiskRecords.from_parts(
                output / ("inspected-" + uuid.uuid4().hex + ".sqlite"), inspected_path)
            rows = inspected_records.view()
        else:
            rows = inspected.collect()
        if len(rows) != len(decisions) or len({r["trajectory_id"] for r in rows}) != len(rows):
            raise ValueError("Source trajectory IDs or count are not unique")
        by_path = inspected_records if large else {row["path"]: row for row in rows}
        if by_path.keys() != decisions.keys():
            raise ValueError("Inspection does not cover all decisions")
        for path, row in by_path.items():
            decision = decisions[path]
            if (row["status"] != decision["status"]
                    or row["final_path"] != decision.get("retained", decision["record"])["path"]
                    or any(row[k] != decision["record"][k] for k in ("sha256", "bytes"))):
                raise ValueError("Inspected row differs from frozen decision")
        kept = inspected_records.view("kept") if large else [row for row in rows if row["status"] == "kept"]
        if len(kept) != prior_report["kept_count"]:
            raise ValueError("Retained count differs from audited count")

        # Freeze lineage bytes before joins; keep the source contract and raw provenance.
        lineage_entry = next(x for x in source["files"] if x["path"] == "lineage.jsonl")
        lineage_path = output / "source-lineage.jsonl"
        if not lineage_path.exists():
            store.download(store.source + "lineage.jsonl", lineage_path, lineage_entry)
        if file_sha256(lineage_path) != lineage_entry["sha256"]:
            raise ValueError("Frozen lineage changed")
        lineage = sc.textFile(str(lineage_path), minPartitions=partitions).filter(bool).map(json.loads)
        lineage_ids = lineage.map(lambda row: row["trajectory_id"])
        if lineage.count() != len(rows) or lineage_ids.distinct().count() != len(rows):
            raise ValueError("Source lineage IDs are not unique")
        by_identifier = inspected.keyBy(lambda row: row["trajectory_id"])
        joined = by_identifier.join(lineage.keyBy(lambda row: row["trajectory_id"]), partitions)
        if joined.count() != len(rows):
            raise ValueError("Source lineage does not cover source trajectories")
        merged = stage(sc, output, "merged-lineage", owner,
                       lambda: joined.values().map(lambda pair: (pair[0]["final_path"], pair))
                       .groupByKey(partitions).map(merge_lineage_group)
                       .map(lambda pair: {"path": pair[0], "lineage": pair[1]}))
        if merged.count() != len(kept):
            raise ValueError("Merged lineage count differs from retained count")
        original_origins = lineage.flatMap(lambda row: ((x["source_ref"], x["sha256"])
                                                       for x in row["origins"])).distinct()
        merged_origins = merged.flatMap(lambda row: ((x["source_ref"], x["sha256"])
                                                     for x in row["lineage"]["origins"])).distinct()
        if original_origins.subtract(merged_origins).take(1) or merged_origins.subtract(original_origins).take(1):
            raise ValueError("Raw source coverage changed during lineage merge")
        origin_count = original_origins.count()
        if origin_count + source["counts"]["skipped_inputs"] + source["counts"]["quarantined_records"] != source["counts"]["input_files"]:
            raise ValueError("Raw source accounting does not reconcile")
        merged_file = output / "lineage.jsonl"
        if not merged_file.exists():
            sorted_lineage = stage(sc, output, "sorted-lineage", owner,
                                   lambda: merged.sortBy(lambda r: r["path"], numPartitions=partitions))
            if sorted_lineage.count() != len(kept):
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
        old_reasons, new_reasons = reason_counts(rows), reason_counts(kept)
        declared = source["counts"]["reason_counts"]
        if any(declared.get(k, 0) < v for k, v in old_reasons.items()):
            raise ValueError("Source reason accounting cannot be separated")
        raw_failure_reasons = Counter({k: v - old_reasons[k] for k, v in declared.items()})
        counts = {**source["counts"],
                  "accepted": sum(row["strict"] for row in kept),
                  "quarantined_trajectories": sum(not row["strict"] for row in kept),
                  "duplicate_trajectories": duplicates,
                  "reason_counts": dict(sorted((raw_failure_reasons + new_reasons).items()))}
        manifest_path = output / "normalized-manifest.json"
        if manifest_path.exists():
            candidate = json.loads(manifest_path.read_text())
        else:
            run_state = {**state, "completed_at_ms": int(time.time() * 1000)}
            files = [{k: row[k] for k in ("path", "bytes", "sha256")} for row in kept]
            candidate = normalized_manifest(source, [*files, merged_entry], counts, run_state)
            _Manifest.model_validate(candidate)
        _Manifest.model_validate(candidate)
        if candidate["counts"] != counts or candidate["config_hash"] != owner:
            raise ValueError("Manifest checkpoint no longer matches inspected inputs")
        emit("publication_preflight_passed", retained=len(kept), selected=sum(r["selected"] for r in kept),
             accepted=counts["accepted"], quarantined=counts["quarantined_trajectories"],
             unique_raw_sources=origin_count)

        kept_input = inspected.filter(lambda r: r["status"] == "kept").map(lambda r: encoded(r).decode())
        resumed_normalized = (output / "stages/normalized-copy.json").exists()
        copies = stage(sc, output, "normalized-copy", owner,
                       lambda: kept_input.mapPartitions(lambda it: copy_partition(it, spec, owner)))
        if resumed_normalized:
            if copies.mapPartitions(lambda it: verify_copies_partition(it, spec, owner)).sum() != len(kept):
                raise ValueError("Resumed normalized outputs failed verification")
        if large:
            copy_path = json.loads((output / "stages/normalized-copy.json").read_text())["path"]
            copy_records = DiskRecords.from_parts(
                output / ("copies-" + uuid.uuid4().hex + ".sqlite"), copy_path)
            copy_rows = copy_records.view()
        else:
            copy_rows = copies.collect()
        if len(copy_rows) != len(kept) or {r["path"] for r in copy_rows} != {r["path"] for r in kept}:
            raise ValueError("Normalized copy receipts are incomplete")
        store.put_file(store.normalized + "lineage.jsonl", merged_file)
        require_inventory(store, store.normalized, {f["path"]: f["bytes"] for f in candidate["files"]})
        # A source manifest change blocks publication even if all copied objects matched.
        source_after, _ = store.read(store.source + "manifest.json", etag=manifest_etag)
        if source_after != raw_manifest:
            raise ValueError("Source changed during publication")
        if not manifest_path.exists():
            candidate["completed_at_ms"] = int(time.time() * 1000)
            write_json(manifest_path, candidate)
        store.put(store.normalized + "manifest.json", encoded(candidate))
        emit("normalized_published", count=len(copy_rows), total_bytes=candidate["total_bytes"])

        selected_rows, model_reports = [], []
        if selection_enabled(spec):
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
        final_source, _ = store.read(store.source + "manifest.json", etag=manifest_etag)
        if final_source != raw_manifest:
            raise ValueError("Source changed before final completion")
        report = {
            "passed": True, "publication_id": owner, "spark_master": sc.master,
            "spark_version": spark.version, "source_uri": spec["source_uri"],
            "normalized_uri": spec["normalized_uri"], "input_count": len(rows),
            "kept_count": len(kept), "removed_count": len(rows) - len(kept),
            "selected_count": len(selected_rows), "normalized_bytes": candidate["total_bytes"],
            "selected_bytes": sum(r["bytes"] for r in selected_rows),
            "accepted": counts["accepted"], "quarantined": counts["quarantined_trajectories"],
            "policy": spec["policy"], "models": model_reports,
            "publish_selection": selection_enabled(spec),
            "comparison_mode": spec.get("comparison_mode", "prefix"),
            "retained_models": dict(sorted(Counter(r["model"] for r in kept).items())),
            "verification": {"source_hashes": len(rows), "normalized_target_hashes": len(copy_rows),
                             "selected_target_hashes": len(selected_rows),
                             "derived_fields_checked": len(kept), "lineage_rows": len(kept),
                             "unique_raw_sources": origin_count, "source_manifest_unchanged": True},
        }
        write_json(output / "report.json", report)
        (output / "SUCCEEDED").write_text("All target bytes, inventories, metrics and lineage checked.\n")
        emit("publication_completed", kept=len(kept), selected=len(selected_rows), models=len(model_reports))
        return report
    finally:
        spark.stop()
