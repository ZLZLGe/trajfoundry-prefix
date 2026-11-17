"""Cluster entrypoint for read-only retained-trajectory diagnostics."""

import argparse
import hashlib
import zipfile
from pathlib import Path

from pyspark.sql import SparkSession

import trajectory_prefix
from trajectory_prefix.deep_review import run
from trajectory_prefix.storage import StorageConfig


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input-plan", required=True, type=Path)
    p.add_argument("--output", required=True, type=Path)
    p.add_argument("--partitions", required=True, type=int)
    p.add_argument("--s3-endpoint", required=True)
    p.add_argument("--s3-region", default="us-east-1")
    p.add_argument("--s3-credentials-file", required=True)
    p.add_argument("--extra-information", choices=["messages_only"], required=True)
    p.add_argument("--output-layout", default="flat")
    p.add_argument("--expected-count", type=int, required=True)
    p.add_argument("--expected-sha", required=True)
    a = p.parse_args()
    spark = SparkSession.builder.appName("trajectory-prefix-retained-deep-review").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    package = Path(trajectory_prefix.__file__).resolve().parent
    code_zip = Path(__file__).resolve().parent / (
        "deep-review-" + hashlib.sha256(str(a.output).encode()).hexdigest()[:12] + ".zip"
    )
    with zipfile.ZipFile(code_zip, "w", zipfile.ZIP_DEFLATED) as archive:
        for file in sorted(package.glob("*.py")):
            archive.write(file, "trajectory_prefix/" + file.name)
    spark.sparkContext.addPyFile(str(code_zip))
    try:
        run(spark, a.input_plan, a.output, partitions=a.partitions,
            storage=StorageConfig(a.s3_endpoint, a.s3_region, a.s3_credentials_file),
            expected_count=a.expected_count, expected_sha=a.expected_sha)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
