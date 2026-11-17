"""Publish an audited normalized selection inside Spark Cluster."""

import argparse

from trajectory_prefix.publication_job import run

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--publication-config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--partitions", type=int, default=512)
    args = parser.parse_args()
    run(args.publication_config, args.output, args.partitions)
