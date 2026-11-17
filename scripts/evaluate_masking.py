"""Evaluate relaxed placeholders in a frozen complete S3 partition."""

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession

from trajectory_prefix.mask_job import run
from trajectory_prefix.storage import StorageConfig

if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input-plan", required=True)
    p.add_argument("--source-manifest-ref", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--partitions", type=int, default=512)
    p.add_argument("--s3-endpoint", required=True)
    p.add_argument("--s3-region", required=True)
    p.add_argument("--s3-credentials-file", required=True)
    a = p.parse_args()
    spark = SparkSession.builder.appName("trajectory-prefix-relaxed-masking").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        run(spark, a.input_plan, a.output,
            StorageConfig(a.s3_endpoint, a.s3_region, a.s3_credentials_file),
            a.partitions, json.loads(Path(a.source_manifest_ref).read_text()))
    finally:
        spark.stop()
