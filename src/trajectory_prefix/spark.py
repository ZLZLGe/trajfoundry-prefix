"""Spark sort/scan pipeline; input bytes never pass through a serializer on output."""

from __future__ import annotations

import json
import tempfile
import zipfile
from collections import Counter
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from .engine import PrefixScan, Record
from .projection import Policy, digest, project
from .storage import Reader, StorageConfig, atomic_bytes, reference


def partition_key(key):
    # Any mergeable prefix has at least user+assistant. The first two tokens
    # distribute large sessionless identities without splitting prefix chains.
    return int.from_bytes(digest(key[0] + key[1][:64])[:8], "big")


def project_partition(entries, policy, storage):
    reader = Reader(storage)
    for entry in entries:
        ref = reference(json.loads(entry))
        p = project(reader.node(ref), ref["dataset"], ref["date"], policy)
        yield Record(ref, p).key, ref


def scan_partition(rows, policy, storage):
    reader = Reader(storage)
    with tempfile.TemporaryDirectory(prefix="trajectory-prefix-") as scratch:
        scan = PrefixScan(Path(scratch) / "prefix.sqlite")
        try:
            for key, ref in rows:
                record = Record(ref, project(reader.node(ref), ref["dataset"], ref["date"], policy))
                if record.key != key:
                    raise RuntimeError("Projection changed between Spark stages")
                scan.insert(record)
            for result in scan.results():
                yield json.dumps(result, ensure_ascii=False, separators=(",", ":"))
        finally:
            scan.close()


def publish_partition(rows, output, storage):
    reader = Reader(storage)
    count = size = 0
    root = Path(output)
    for line in rows:
        row = json.loads(line)
        if row["status"] != "kept":
            continue
        ref = row["record"]
        target = root / "trajectories" / ref["id"]
        if ref.get("local_path") and (
            Path(ref["local_path"]).resolve() == target.resolve()
            or root in Path(ref["local_path"]).resolve().parents
        ):
            raise ValueError("Output must be separate from input")
        data = reader.read(ref)
        atomic_bytes(target, data, ref["sha256"])
        count += 1
        size += len(data)
    yield count, size


def aggregate_counts(rows):
    counts = Counter()
    for line in rows:
        row = json.loads(line)
        ref = row["record"]
        for source in ("all", ref["dataset"]):
            counts[(source, "input")] += 1
            counts[(source, row["status"])] += 1
            if row.get("isolation_reason"):
                counts[(source, "isolated:" + row["isolation_reason"])] += 1
    yield from counts.items()


def finish(output, report):
    (output / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    (output / "SUCCEEDED").write_text(datetime.now(UTC).isoformat() + "\n")
    (output / "RUNNING").unlink()
    return report


def run(
    spark,
    input_plan: Path,
    output: Path,
    *,
    partitions=64,
    policy=Policy(),
    storage=StorageConfig(),
    check_only=False,
):
    output = output.resolve()
    if str(output).startswith(("s3:", "s3a:")):
        raise ValueError("This program writes only to a local/shared filesystem")
    output.mkdir(parents=True, exist_ok=False)
    (output / "RUNNING").write_text(datetime.now(UTC).isoformat() + "\n")
    sc = spark.sparkContext
    if not sc.master.startswith("local") and str(output).startswith(("/tmp/", "/home/")):
        raise ValueError("Cluster mode needs a shared output directory visible to all executors")
    # Cluster mode rejects registering the same archive basename from another
    # path when a second pass runs in the same SparkContext.
    code_zip = output / ("package-" + digest(str(output).encode()).hex()[:16] + ".zip")
    package = Path(__file__).parent
    with zipfile.ZipFile(code_zip, "w", zipfile.ZIP_DEFLATED) as z:
        for path in sorted(package.glob("*.py")):
            z.write(path, "trajectory_prefix/" + path.name)
    sc.addPyFile(str(code_zip))
    inputs = sc.textFile(str(input_plan.resolve()), minPartitions=partitions).filter(
        lambda x: bool(x.strip())
    )
    # A duplicate input reference would otherwise cause output-count ambiguity.
    input_count = inputs.count()
    distinct_count = (
        inputs.map(lambda line: reference(json.loads(line))["id"]).distinct(partitions).count()
    )
    if distinct_count != input_count:
        raise ValueError("Input plan contains duplicate dataset/date/path references")
    projected = inputs.mapPartitions(lambda rows: project_partition(rows, policy, storage))
    sorted_rows = projected.repartitionAndSortWithinPartitions(
        partitions, partitionFunc=partition_key
    )
    decisions_path = output / "decisions"
    sorted_rows.mapPartitions(lambda rows: scan_partition(rows, policy, storage)).saveAsTextFile(
        str(decisions_path)
    )
    decisions = sc.textFile(str(decisions_path), minPartitions=partitions)
    counts = dict(
        decisions.mapPartitions(aggregate_counts).reduceByKey(lambda a, b: a + b).collect()
    )
    if counts.get(("all", "input")) != input_count:
        raise RuntimeError("Decision count does not match input count")
    kept_count = counts.get(("all", "kept"), 0)
    report = {
        "spark_version": spark.version,
        "spark_master": sc.master,
        "policy": asdict(policy),
        "input_plan": str(input_plan.resolve()),
        "input_count": input_count,
        "kept_count": kept_count,
        "removed_count": input_count - kept_count,
        "counts": {
            source: {field: n for (s, field), n in counts.items() if s == source}
            for source in sorted({s for s, _ in counts})
        },
    }
    if check_only:
        report["check_only"] = True
        return finish(output, report)
    results = decisions.mapPartitions(
        lambda rows: publish_partition(rows, str(output), storage)
    ).collect()
    kept_count, kept_bytes = sum(x[0] for x in results), sum(x[1] for x in results)
    if kept_count != counts.get(("all", "kept"), 0):
        raise RuntimeError("Published count does not match decisions")
    # Stream a conventional file manifest and a runnable retained-input plan;
    # neither is embedded in trajectory records.
    with (
        (output / "manifest.json").open("w") as manifest,
        (output / "retained-input.jsonl").open("w") as plan,
    ):
        manifest.write(
            '{"trajectory_count":'
            + str(kept_count)
            + ',"total_bytes":'
            + str(kept_bytes)
            + ',"files":['
        )
        first = True
        for part in sorted(decisions_path.glob("part-*")):
            with part.open() as handle:
                for line in handle:
                    row = json.loads(line)
                    if row["status"] != "kept":
                        continue
                    ref = row["record"]
                    file = {
                        "path": "trajectories/" + ref["id"],
                        "bytes": ref["bytes"],
                        "sha256": ref["sha256"],
                    }
                    manifest.write(("" if first else ",") + json.dumps(file, separators=(",", ":")))
                    first = False
                    plan.write(
                        json.dumps(
                            dict(ref, local_path=str(output / file["path"])),
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                        + "\n"
                    )
        manifest.write("]}\n")
    report["output_bytes"] = kept_bytes
    return finish(output, report)
