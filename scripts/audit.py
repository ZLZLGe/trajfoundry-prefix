"""Audit every retained file and every deletion using an independent raw-object verifier."""

import argparse
import hashlib
import json
from collections import Counter
from dataclasses import replace
from pathlib import Path

from pyspark.sql import SparkSession

from trajectory_prefix.projection import Policy
from trajectory_prefix.residuals import review_files
from trajectory_prefix.storage import Reader, StorageConfig, output_relative_path, reference
from trajectory_prefix.verification import raw_covers


def identity(entry):
    r = reference(entry)
    return r["id"], (r["bytes"], r["sha256"])


def verify_partition(lines, source, storage, policy, layout="flat"):
    reader = Reader(storage)
    output_reader = Reader(StorageConfig())
    counts = Counter()
    for line in lines:
        decision = json.loads(line)
        ref = reference(decision["record"])
        source_key = ref["dataset"] + "/" + ref["date"]
        group = ("all", ref["dataset"], source_key)
        for g in group:
            counts[(g, "input")] += 1
            counts[(g, decision["status"])] += 1
        if decision["status"] == "kept":
            target = dict(ref, local_path=str(Path(source) / output_relative_path(ref, layout)))
            # Disallow S3 fallback when proving that the output actually exists.
            target.pop("uri", None)
            output_reader.read(target)
            for g in group:
                counts[(g, "output_hash_verified")] += 1
        elif decision["status"] == "removed":
            retained = reference(decision["retained"])
            target = dict(
                retained, local_path=str(Path(source) / output_relative_path(retained, layout))
            )
            target.pop("uri", None)
            short, long = reader.node(ref), output_reader.node(target)
            if not raw_covers(short, long, ref, retained, policy):
                raise RuntimeError("Independent witness verification failed: " + ref["id"])
            strict = raw_covers(short, long, ref, retained, replace(policy, rules=()))
            required = []
            for rule in policy.rules:
                without = replace(policy, rules=tuple(r for r in policy.rules if r != rule))
                if not raw_covers(short, long, ref, retained, without):
                    required.append(rule)
            for g in group:
                counts[(g, "witness_verified")] += 1
                counts[(g, "strict_witness" if strict else "robust_witness")] += 1
                counts[(g, "required:" + ("+".join(required) or "none"))] += 1
        else:
            raise ValueError("Unknown decision status")
    yield from counts.items()


def verify_metadata(source, report):
    # Reconstruct the small manifest contract from the independent decision
    # stream without loading all trajectory payloads or a giant JSON tree.
    expected = hashlib.sha256()
    expected.update(
        (
            '{"trajectory_count":'
            + str(report["kept_count"])
            + ',"total_bytes":'
            + str(report["output_bytes"])
            + ',"files":['
        ).encode()
    )
    first = True
    rows = size = 0
    for path in sorted((source / "decisions").glob("part-*")):
        with path.open() as handle:
            for line in handle:
                row = json.loads(line)
                if row["status"] != "kept":
                    continue
                ref = reference(row["record"])
                file = {
                    "path": output_relative_path(ref, report.get("output_layout", "flat")),
                    "bytes": ref["bytes"],
                    "sha256": ref["sha256"],
                }
                expected.update(
                    (("" if first else ",") + json.dumps(file, separators=(",", ":"))).encode()
                )
                first = False
                rows += 1
                size += ref["bytes"]
    expected.update(b"]}\n")
    actual = hashlib.sha256()
    with (source / "manifest.json").open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            actual.update(block)
    if (
        expected.digest() != actual.digest()
        or rows != report["kept_count"]
        or size != report["output_bytes"]
    ):
        raise RuntimeError("Manifest entries/counts do not agree with decisions")
    return actual.hexdigest()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--source-run", required=True, type=Path)
    p.add_argument("--output", required=True, type=Path)
    p.add_argument("--partitions", type=int, default=512)
    p.add_argument("--s3-endpoint")
    p.add_argument("--s3-region", default="us-east-1")
    p.add_argument("--s3-credentials-file")
    p.add_argument("--cache-dir")
    a = p.parse_args()
    source, output = a.source_run.resolve(), a.output.resolve()
    if not (source / "SUCCEEDED").is_file():
        raise RuntimeError("Source run is incomplete")
    output.mkdir(parents=True, exist_ok=False)
    initial = json.loads((source / "report.json").read_text())
    if initial.get("check_only"):
        raise RuntimeError("A check-only run has no published trajectories to audit")
    policy_config = dict(initial["policy"])
    policy_config["rules"] = tuple(policy_config["rules"])
    policy_config.setdefault("extra_information", "preserve_all")
    policy = Policy(**policy_config)
    layout = initial.get("output_layout", "flat")
    storage = StorageConfig(a.s3_endpoint, a.s3_region, a.s3_credentials_file, a.cache_dir)
    manifest_sha = verify_metadata(source, initial)
    spark = SparkSession.builder.appName("trajectory-prefix-audit").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    try:
        decisions = sc.textFile(str(source / "decisions"), minPartitions=a.partitions)
        original = (
            sc.textFile(initial["input_plan"], minPartitions=a.partitions)
            .filter(lambda s: s.strip())
            .map(lambda line: identity(json.loads(line)))
        )
        decision_refs = decisions.map(lambda line: identity(json.loads(line)["record"]))
        count = decision_refs.count()
        if (
            count != initial["input_count"]
            or decision_refs.keys().distinct(a.partitions).count() != count
        ):
            raise RuntimeError("Missing or duplicated decisions")
        if (
            original.fullOuterJoin(decision_refs, a.partitions)
            .filter(lambda row: row[1][0] != row[1][1])
            .take(1)
        ):
            raise RuntimeError("Decision inventory differs from the original input plan")
        kept = decisions.filter(lambda line: json.loads(line)["status"] == "kept").map(
            lambda line: identity(json.loads(line)["record"])
        )
        plan = sc.textFile(str(source / "retained-input.jsonl"), minPartitions=a.partitions).map(
            lambda line: identity(json.loads(line))
        )
        if (
            plan.count() != initial["kept_count"]
            or plan.keys().distinct(a.partitions).count() != initial["kept_count"]
        ):
            raise RuntimeError("Retained plan contains missing or duplicate entries")
        if (
            kept.fullOuterJoin(plan, a.partitions)
            .filter(lambda row: row[1][0] != row[1][1])
            .take(1)
        ):
            raise RuntimeError("Retained plan differs from kept decisions")
        removed_targets = decisions.filter(
            lambda line: json.loads(line)["status"] == "removed"
        ).map(lambda line: identity(json.loads(line)["retained"]))
        if (
            removed_targets.leftOuterJoin(kept, a.partitions)
            .filter(lambda row: row[1][0] != row[1][1])
            .take(1)
        ):
            raise RuntimeError("Deletion points to a non-retained or different witness")
        counts = dict(
            decisions.mapPartitions(
                lambda rows: verify_partition(rows, str(source), storage, policy, layout)
            )
            .reduceByKey(lambda x, y: x + y)
            .collect()
        )
        if (
            counts.get(("all", "output_hash_verified"), 0) != initial["kept_count"]
            or counts.get(("all", "witness_verified"), 0) != initial["removed_count"]
        ):
            raise RuntimeError("Not all outputs and witnesses were checked")
        parts = [str(path) for path in sorted((source / "decisions").glob("part-*"))]
        residual = dict(
            sc.parallelize(parts, min(a.partitions, len(parts)) or 1)
            .mapPartitions(
                lambda paths: review_files(paths, str(source), str(output), policy, layout)
            )
            .reduceByKey(lambda x, y: x + y)
            .collect()
        )
        report = {
            "passed": True,
            "spark_master": sc.master,
            "spark_version": spark.version,
            "source_run": str(source),
            "manifest_sha256": manifest_sha,
            "counts": {
                group: {k: v for (g, k), v in counts.items() if g == group}
                for group in sorted({g for g, k in counts})
            },
            "residual_review": {
                "definition": "Adjacent retained prefixes within each original sorted partition; at least four messages and 80% common prefix of the shorter record.",
                "limitation": "This does not discover every possible alternative normalization, particularly differences in the first two messages.",
                "counts": {
                    group: {k: v for (g, k), v in residual.items() if g == group}
                    for group in sorted({g for g, k in residual})
                },
            },
        }
        (output / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        (output / "SUCCEEDED").write_text("all outputs and witnesses verified\n")
        print(json.dumps(report), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
