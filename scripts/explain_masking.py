"""Explain all selected placeholder witnesses inside the user Cluster."""

import argparse
import json
import zipfile
from pathlib import Path

from pyspark.sql import SparkSession

import trajectory_prefix
from trajectory_prefix.mask_evidence import explain_partition, summarize_partition
from trajectory_prefix.mask_job import nested_counts
from trajectory_prefix.publication import write_json
from trajectory_prefix.storage import StorageConfig

if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source-run", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--partitions", type=int, default=512)
    p.add_argument("--s3-endpoint", required=True)
    p.add_argument("--s3-region", required=True)
    p.add_argument("--s3-credentials-file", required=True)
    a = p.parse_args()
    spark = SparkSession.builder.appName("trajectory-prefix-mask-evidence").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    try:
        if not sc.master.startswith("k8s://"):
            raise ValueError("Business evidence requires Spark Cluster")
        source = Path(a.source_run)
        report = json.loads((source / "report.json").read_text())
        if not report["passed"] or not (source / "SUCCEEDED").is_file():
            raise ValueError("Completed evaluated source required")
        output = Path(a.output)
        output.mkdir(parents=True, exist_ok=False)
        package = output / "evidence-package.zip"
        with zipfile.ZipFile(package, "w", zipfile.ZIP_DEFLATED) as z:
            for f in Path(trajectory_prefix.__file__).parent.glob("*.py"):
                z.write(f, "trajectory_prefix/" + f.name)
        sc.addPyFile(str(package))
        storage = StorageConfig(a.s3_endpoint, a.s3_region, a.s3_credentials_file)
        decisions = sc.textFile(str(source / "decisions")).filter(bool).map(json.loads)
        target = output / "witnesses"
        decisions.filter(lambda r: r["status"] == "removed").repartition(a.partitions).mapPartitions(
            lambda it: explain_partition(it, storage)).map(lambda x: json.dumps(x, ensure_ascii=False)).saveAsTextFile(str(target))
        rows = sc.textFile(str(target)).filter(bool).map(json.loads)
        counts = nested_counts(rows.mapPartitions(summarize_partition).reduceByKey(lambda x, y: x + y).collect())
        if counts.get("all", {}).get("removed", 0) != report["removed_count"]:
            raise ValueError("Incomplete removal explanations")
        final = {"passed": True, "source_run": str(source), "counts": counts, "s3_writes": False}
        write_json(output / "report.json", final)
        (output / "SUCCEEDED").write_text("All removal explanations independently checked.\n")
        print(json.dumps(final), flush=True)
    finally:
        spark.stop()
