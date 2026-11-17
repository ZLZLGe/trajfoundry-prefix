"""Refine a full-date ordinary-prefix baseline and audit direct final witnesses."""

import json
import time
import zipfile
from dataclasses import asdict
from pathlib import Path

from .deep_review import NodeReader
from .mask_evidence import explain_partition, summarize_partition
from .mask_job import POLICY, check_partition, index_partition, nested_counts
from .mask_verification import raw_relaxed_covers
from .masking import MAX_SPAN, TEMPLATES, VERSION, choose_group, group_candidates
from .publication import encoded, write_json
from .spark import run as prefix_run
from .storage import Reader, file_sha256


def rows(sc, path, partitions):
    return sc.textFile(str(path), minPartitions=partitions).filter(bool).map(json.loads)


def save(data, path):
    data.map(lambda r: encoded(r).decode().rstrip("\n")).saveAsTextFile(str(path))


def export_kept(decision_dir, path):
    count = 0
    with Path(path).open("xb") as out:
        for part in sorted(Path(decision_dir).glob("part-*")):
            with part.open() as stream:
                for line in stream:
                    row = json.loads(line)
                    if row["status"] == "kept":
                        out.write(encoded(row["record"]))
                        count += 1
    return count


def compose(pair):
    original, masked = pair
    if masked is None:
        raise ValueError("Baseline representative missing from masking decisions")
    row = {**original, "model": masked["model"]}
    if masked["status"] == "removed":
        row.update(status="removed", retained=masked["retained"],
                   reason="placeholder_relaxed" if original["status"] == "kept"
                   else "prefix_then_placeholder")
    return row


def direct_audit(data, storage):
    reader = NodeReader(storage)
    for row in data:
        if row["status"] != "removed":
            continue
        ar, br = row["record"], row["retained"]
        passed = raw_relaxed_covers(reader.node(ar), reader.node(br), ar, br, POLICY)
        yield {"record": ar, "retained": br, "passed": passed}


def run(spark, input_plan, baseline_path, output, storage, partitions, manifest_ref):
    import trajectory_prefix

    sc = spark.sparkContext
    if not sc.master.startswith("k8s://"):
        raise ValueError("Full-date refinement requires Spark Cluster")
    started = time.monotonic()
    output, baseline_path = Path(output), Path(baseline_path)
    output.mkdir(parents=True, exist_ok=False)

    def event(name, **fields):
        value = {"event": name, "elapsed_seconds": round(time.monotonic() - started, 2), **fields}
        with (output / "progress.jsonl").open("ab") as f:
            f.write(encoded(value))
        print(json.dumps(value), flush=True)

    package = output / "refinement-package.zip"
    with zipfile.ZipFile(package, "w", zipfile.ZIP_DEFLATED) as z:
        for f in Path(trajectory_prefix.__file__).parent.glob("*.py"):
            z.write(f, "trajectory_prefix/" + f.name)
    sc.addPyFile(str(package))
    baseline = json.loads((baseline_path / "report.json").read_text())
    expected_policy = json.loads(json.dumps(asdict(POLICY)))
    plan_hash = file_sha256(Path(input_plan))
    if (not (baseline_path / "SUCCEEDED").is_file()
            or not (baseline_path / "decisions/_SUCCESS").is_file()
            or baseline["policy"] != expected_policy
            or baseline["input_plan_sha256"] != plan_hash):
        raise ValueError("Baseline is not the frozen full-date messages-only run")
    Reader(storage).read(manifest_ref)
    baseline_input = output / "baseline-input.jsonl"
    if export_kept(baseline_path / "decisions", baseline_input) != baseline["kept_count"]:
        raise ValueError("Incomplete baseline selection")
    event("baseline_recheck_started", count=baseline["kept_count"])
    recheck = prefix_run(spark, baseline_input, output / "baseline-recheck", partitions=partitions,
                        policy=POLICY, storage=storage, check_only=True)
    if recheck["removed_count"]:
        raise ValueError("Ordinary baseline has residual prefixes")
    index_path = output / "index"
    sc.textFile(str(baseline_input), minPartitions=partitions).filter(bool).mapPartitions(
        lambda it: index_partition(it, storage)).saveAsPickleFile(str(index_path))
    indexed = sc.pickleFile(str(index_path), minPartitions=partitions)
    if indexed.count() != baseline["kept_count"]:
        raise ValueError("Baseline index incomplete")
    event("baseline_index_complete", count=baseline["kept_count"])

    def compare(index, name):
        candidates_path, checks_path = output / (name + "-candidates"), output / (name + "-checks")
        candidates = index.filter(lambda r: not r["isolation"]).keyBy(lambda r: r["group"]).groupByKey(
            partitions).flatMap(group_candidates)
        save(candidates, candidates_path)
        candidates = rows(sc, candidates_path, partitions)
        count = candidates.count()
        event(name + "_candidates", count=count)
        save(candidates.repartition(partitions).mapPartitions(lambda it: check_partition(it, storage)), checks_path)
        checked = rows(sc, checks_path, partitions)
        stats = dict(checked.map(lambda r: ((r["matched"], r["outside_bound_only"]), 1)).reduceByKey(
            lambda a, b: a + b).collect())
        if sum(stats.values()) != count:
            raise ValueError("Candidate accounting mismatch")
        event(name + "_compared", candidates=count, matched=stats.get((True, False), 0))
        return checked, count, stats

    checked, candidate_count, stats = compare(indexed, "main")
    edges = checked.filter(lambda r: r["matched"]).keyBy(lambda r: r["group"])
    masked = indexed.keyBy(lambda r: r["group"]).cogroup(edges, partitions).flatMap(choose_group)
    mask_path = output / "mask-decisions"
    save(masked, mask_path)
    masked = rows(sc, mask_path, partitions)
    additional = masked.filter(lambda r: r["status"] == "removed").count()
    original = rows(sc, baseline_path / "decisions", partitions)
    mapped = original.keyBy(lambda r: r.get("retained", r["record"])["id"]).leftOuterJoin(
        masked.keyBy(lambda r: r["record"]["id"]), partitions).values().map(compose)
    preliminary = output / "provisional-decisions"
    save(mapped, preliminary)
    save(rows(sc, preliminary, partitions).mapPartitions(lambda it: direct_audit(it, storage)),
         output / "provisional-audit")
    audit = rows(sc, output / "provisional-audit", partitions)
    failed = audit.filter(lambda r: not r["passed"]).map(lambda r: (r["record"]["id"], True))
    restored = failed.count()

    def repair(pair):
        row, failed_witness = pair
        if failed_witness:
            row = {**row, "status": "kept", "reason": "direct_witness_not_proven"}
            row.pop("retained", None)
        return row

    final = rows(sc, preliminary, partitions).keyBy(lambda r: r["record"]["id"]).leftOuterJoin(
        failed, partitions).values().map(repair)
    save(final, output / "decisions")
    save(audit.filter(lambda r: r["passed"]), output / "audit")
    decisions = rows(sc, output / "decisions", partitions)
    totals = dict(decisions.map(lambda r: (r["status"], 1)).reduceByKey(lambda a, b: a + b).collect())
    kept_count = totals.get("kept", 0)
    if sum(totals.values()) != baseline["input_count"]:
        raise ValueError("Final decisions do not cover all original inputs")
    kept = decisions.filter(lambda r: r["status"] == "kept").map(lambda r: r["record"])
    if decisions.filter(lambda r: r["status"] == "removed").map(
        lambda r: (r["retained"]["id"], 1)).subtractByKey(kept.map(lambda r: (r["id"], 1))).take(1):
        raise ValueError("Final witness was not retained")
    if audit.filter(lambda r: r["passed"]).count() != totals.get("removed", 0):
        raise ValueError("Independent audit does not cover final removals")
    save(kept, output / "retained-input")
    final_input = output / "final-input.jsonl"
    if export_kept(output / "decisions", final_input) != kept_count:
        raise ValueError("Final retained selection incomplete")
    event("direct_audit_complete", kept=kept_count, removed=totals.get("removed", 0), restored=restored)
    ordinary_final = prefix_run(spark, final_input, output / "ordinary-final-recheck",
                               partitions=partitions, policy=POLICY, storage=storage, check_only=True)
    if ordinary_final["removed_count"]:
        raise ValueError("Final retained ordinary comparison has additional matches")
    reindex_path = output / "recheck-index"
    rows(sc, output / "retained-input", partitions).mapPartitions(
        lambda it: index_partition(it, storage)).saveAsPickleFile(str(reindex_path))
    reindexed = sc.pickleFile(str(reindex_path), minPartitions=partitions)
    if reindexed.count() != kept_count:
        raise ValueError("Retained reread incomplete")
    _, recheck_count, recheck_stats = compare(reindexed, "recheck")
    if recheck_stats.get((True, False), 0):
        raise ValueError("Retained relaxed comparison has additional matches")
    # Attribute only the incremental masking decisions, not ordinary prefixes.
    save(masked.repartition(partitions).mapPartitions(lambda it: explain_partition(it, storage)),
         output / "explanations")
    explanations = rows(sc, output / "explanations", partitions)
    attribution = nested_counts(explanations.mapPartitions(summarize_partition).reduceByKey(
        lambda a, b: a + b).collect())
    if explanations.count() != additional:
        raise ValueError("Mask attribution incomplete")
    models = nested_counts(decisions.map(lambda r: ((r["model"], r["status"]), 1)).reduceByKey(
        lambda a, b: a + b).collect())
    Reader(storage).read(manifest_ref)
    report = {"passed": True, "input_count": baseline["input_count"], "kept_count": kept_count,
              "removed_count": totals.get("removed", 0), "baseline_removed": baseline["removed_count"],
              "mask_additional_removed": additional, "restored_direct_witness_failures": restored,
              "candidate_pairs": candidate_count, "matched_pairs": stats.get((True, False), 0),
              "models": models, "attribution": attribution, "policy": expected_policy,
              "mask_version": VERSION, "max_placeholder_chars": MAX_SPAN, "literal_templates": sorted(TEMPLATES),
              "source_manifest_sha256": manifest_ref["sha256"], "input_plan_sha256": plan_hash,
              "verification": {"source_hashes": baseline["input_count"],
                               "removed_witnesses": totals.get("removed", 0),
                               "retained_recheck_hashes": kept_count,
                               "retained_recheck_candidates": recheck_count,
                               "retained_recheck_matches": 0, "ordinary_recheck_matches": 0,
                               "source_manifest_unchanged": True},
              "spark_master": sc.master, "spark_version": spark.version, "s3_writes": False,
              "elapsed_seconds": round(time.monotonic() - started, 2)}
    write_json(output / "report.json", report)
    (output / "SUCCEEDED").write_text("Direct final witnesses and both full rechecks passed.\n")
    event("completed", input=report["input_count"], kept=kept_count, removed=report["removed_count"])
    return report
