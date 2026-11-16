"""Run with python -m trajectory_prefix or via spark-submit scripts/run.py."""

import argparse
import json
from pathlib import Path

from .projection import Policy
from .spark import run
from .storage import StorageConfig


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-plan", type=Path, required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--master", default=None)
    parser.add_argument("--partitions", type=int, default=64)
    parser.add_argument(
        "--rules",
        default=",".join(Policy().rules),
        help="Comma separated rules; empty disables robustness rules",
    )
    parser.add_argument("--s3-endpoint")
    parser.add_argument("--s3-region", default="us-east-1")
    parser.add_argument("--s3-credentials-file")
    parser.add_argument("--cache-dir")
    args = parser.parse_args()
    if "://" in args.output or not Path(args.output).is_absolute():
        parser.error("--output must be an absolute local/shared filesystem path")
    if args.partitions < 1:
        parser.error("--partitions must be positive")
    rules = tuple(filter(None, args.rules.split(",")))
    if set(rules) - set(Policy().rules):
        parser.error("Unknown comparison rule")
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName("trajectory-prefix")
    if args.master:
        builder = builder.master(args.master)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        report = run(
            spark,
            args.input_plan,
            Path(args.output),
            partitions=args.partitions,
            policy=Policy(rules),
            storage=StorageConfig(
                args.s3_endpoint, args.s3_region, args.s3_credentials_file, args.cache_dir
            ),
        )
        print(json.dumps(report, ensure_ascii=False), flush=True)
    finally:
        spark.stop()
