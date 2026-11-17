"""Local-only diagnostic for system/developer messages inserted inside history.

Removing these messages is deliberately too broad for a production rule. It is
only used to measure candidates for manual investigation, not to delete files.
"""
import argparse
import json
import os
import time
import zipfile
from pathlib import Path

import audit_core as audit
import local_runtime
import run_audit

from trajectory_prefix.storage import StorageConfig

MODE = 'without_system_developer'


def convert(row):
    vector = row['variants'].get('stable_messages', b'')
    if not vector:
        return {**row, 'variants': {}}
    assert len(vector) == len(row['roles']) * 32
    selected = b''.join(vector[i * 32:(i + 1) * 32] for i, role in enumerate(row['roles']) if role not in 'SD')
    return {**row, 'variants': {MODE: selected} if len(selected) >= 64 else {}}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-plan', required=True)
    parser.add_argument('--expected-count', required=True, type=int)
    parser.add_argument('--input-index', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--local-workers', type=int, required=True)
    parser.add_argument('--partitions', type=int, required=True)
    parser.add_argument('--s3-endpoint', required=True)
    parser.add_argument('--s3-region', required=True)
    parser.add_argument('--s3-credentials-file', required=True)
    args = parser.parse_args()
    assert args.local_workers > 0, 'This additional experiment must run locally'
    assert (Path(args.input_index) / '_SUCCESS').is_file()
    os.umask(0o077)
    spark = local_runtime.spark_session('local-mid-history-system-probe', args.local_workers)
    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=False)
    start = time.monotonic()
    storage = StorageConfig(endpoint_url=args.s3_endpoint, region=args.s3_region,
                            credentials_file=args.s3_credentials_file)
    archive = out / 'independent-structure.zip'
    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as z:
        for module in (audit, run_audit, local_runtime):
            z.write(module.__file__, Path(module.__file__).name)
    sc.addPyFile(str(archive))
    try:
        indexed = sc.pickleFile(args.input_index, minPartitions=args.partitions)
        count = indexed.count()
        assert count == args.expected_count
        sorted_rows = indexed.map(convert).flatMap(audit.sort_entries).repartitionAndSortWithinPartitions(
            numPartitions=args.partitions, partitionFunc=audit.partition_key)
        run_audit.save(sorted_rows.mapPartitions(audit.neighbors), out / 'candidates')
        candidates = run_audit.lines(sc, out / 'candidates', args.partitions)
        run_audit.save(candidates.repartition(args.partitions).mapPartitions(
            lambda it: run_audit.verify_partition(it, storage)), out / 'verified')
        verified = run_audit.lines(sc, out / 'verified', args.partitions)
        counts = verified.map(lambda r: ((r['short']['dataset'], r['literal_coverage'],
            r['sessionless']), 1)).reduceByKey(lambda a, b: a + b, args.partitions).collect()
        assert sum(v for _, v in counts) == candidates.count()
        examples = verified.filter(lambda r: not r['literal_coverage']).map(lambda r: (
            (r['short']['dataset'], r['sessionless']), [r])).reduceByKey(
                audit.bounded, args.partitions).flatMap(lambda r: r[1])
        run_audit.save(examples.mapPartitions(lambda it: run_audit.verify_partition(it, storage, out / 'evidence')),
                       out / 'examples')
        report = {'passed': True, 'diagnostic_only': True, 's3_writes': False, 'input_count': count,
                  'mode': MODE, 'spark_master': sc.master,
                  'verified_counts': [[list(k), v] for k, v in sorted(counts)],
                  'elapsed_seconds': round(time.monotonic() - start, 2),
                  'limitation': 'Ignoring all system/developer messages can erase real instructions; candidates are not removals.'}
        (out / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
        (out / 'SUCCEEDED').write_text('Local structure probe completed.\n')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
