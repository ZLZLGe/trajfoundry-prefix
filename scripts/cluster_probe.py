"""Check shared mounts, native dependencies and immutable Spark output in ARM Pods."""

import argparse
import hashlib
import json
import os
import platform
import socket
from pathlib import Path

from pyspark.sql import SparkSession

from trajectory_prefix.spark import run


def check_partition(rows, root):
    import boto3
    import orjson

    root = Path(root)
    assert (root / "shared-marker").read_text() == "prefix-cluster-probe\n"
    for i in rows:
        result = {
            "host": socket.gethostname(),
            "arch": platform.machine(),
            "uid": os.geteuid(),
            "boto3": boto3.__version__,
            "orjson": orjson.__version__,
        }
        assert result["arch"] in {"arm64", "aarch64"}
        (root / f"executor-{i}.json").write_text(json.dumps(result))
        yield result


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    root = a.output
    root.mkdir(parents=True, exist_ok=False)
    (root / "shared-marker").write_text("prefix-cluster-probe\n")
    spark = SparkSession.builder.appName("trajectory-prefix-probe").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        runtime = (
            spark.sparkContext.parallelize(range(8), 8)
            .mapPartitions(lambda rows: check_partition(rows, str(root)))
            .collect()
        )
        assert all((root / f"executor-{i}.json").exists() for i in range(8))
        records = []
        messages = [{"role": "user", "content": "check"}, {"role": "assistant", "content": "done"}]
        for i in range(3):
            node = {
                "metadata": {"session_id": "probe", "user_id": "probe"},
                "model": "probe",
                "messages": messages
                + (
                    [{"role": "user", "content": "more"}, {"role": "assistant", "content": "ok"}]
                    if i
                    else []
                ),
            }
            data = (json.dumps(node, indent=i + 1) + "\n").encode()
            path = root / f"input-{i}.json"
            path.write_bytes(data)
            records.append(
                {
                    "dataset": "probe",
                    "date": "archive",
                    "path": path.name,
                    "local_path": str(path),
                    "bytes": len(data),
                    "sha256": hashlib.sha256(data).hexdigest(),
                }
            )
        plan = root / "input.jsonl"
        plan.write_text("".join(json.dumps(r) + "\n" for r in records))
        first = run(spark, plan, root / "first", partitions=4)
        second = run(spark, root / "first/retained-input.jsonl", root / "second", partitions=4)
        assert first["removed_count"] == 2 and second["removed_count"] == 0
        for row in json.loads((root / "first/manifest.json").read_text())["files"]:
            assert (root / "first" / row["path"]).read_bytes() in [
                Path(r["local_path"]).read_bytes() for r in records
            ]
        report = {
            "passed": True,
            "spark_master": spark.sparkContext.master,
            "spark_version": spark.version,
            "runtimes": runtime,
            "first": first,
            "second": second,
        }
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        (root / "SUCCEEDED").write_text("passed\n")
        print(json.dumps(report), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
