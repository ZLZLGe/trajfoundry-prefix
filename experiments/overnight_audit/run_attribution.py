"""Cluster-only explanation of verified candidates; no production changes."""
import argparse
import hashlib
import json
import os
import time
import zipfile
from pathlib import Path

import attribution_core
import audit_core
import run_audit

from trajectory_prefix.storage import Reader, StorageConfig, atomic_bytes


def pair(row):
    return ((row['short']['id'], row['long']['id']),
            {k: row[k] for k in ('short', 'long')} | {'modes': [row['mode']]})


def explain_partition(rows, storage):
    reader = Reader(storage)
    for row in rows:
        yield attribution_core.explain(row, reader.node(row['short']), reader.node(row['long']))


def evidence_partition(rows, storage, destination):
    reader = Reader(storage)
    for row in rows:
        for side in ('short', 'long'):
            ref = row[side]
            atomic_bytes(Path(destination) / (ref['sha256'] + '.json'), reader.read(ref), ref['sha256'])
        yield row


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-plan', required=True)
    parser.add_argument('--expected-count', type=int, required=True)
    parser.add_argument('--input-index', required=True)
    parser.add_argument('--input-coverage', required=True)
    parser.add_argument('--input-empty-coverage', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--partitions', type=int, required=True)
    parser.add_argument('--s3-endpoint', required=True)
    parser.add_argument('--s3-region', required=True)
    parser.add_argument('--s3-credentials-file', required=True)
    args = parser.parse_args()
    for name in ('input_index', 'input_coverage', 'input_empty_coverage'):
        assert (Path(getattr(args, name)) / '_SUCCESS').is_file()
    from pyspark.sql import SparkSession

    os.umask(0o077)
    spark = SparkSession.builder.appName('independent-candidate-attribution').getOrCreate()
    sc = spark.sparkContext
    assert sc.master.startswith('k8s://')
    sc.setLogLevel('WARN')
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    storage = StorageConfig(endpoint_url=args.s3_endpoint, region=args.s3_region,
                            credentials_file=args.s3_credentials_file)
    archive = out / 'independent-attribution.zip'
    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as target:
        for module in (audit_core, attribution_core, run_audit):
            target.write(module.__file__, Path(module.__file__).name)
    sc.addPyFile(str(archive))
    partitions = args.partitions
    try:
        count = sc.pickleFile(args.input_index, minPartitions=partitions).count()
        assert count == args.expected_count
        rows = run_audit.lines(sc, args.input_coverage, partitions).union(
            run_audit.lines(sc, args.input_empty_coverage, partitions))
        unique = rows.map(pair).reduceByKey(attribution_core.combine, partitions).values()
        run_audit.save(unique, out / 'unique-pairs')
        unique_count = run_audit.lines(sc, out / 'unique-pairs', partitions).count()
        run_audit.save(run_audit.lines(sc, out / 'unique-pairs', partitions).mapPartitions(
            lambda it: explain_partition(it, storage)), out / 'attributed')
        result = run_audit.lines(sc, out / 'attributed', partitions)
        actual = result.count()
        assert actual == unique_count
        reason_counts = result.map(lambda r: ((r['short']['dataset'], r['reason']), 1)).reduceByKey(
            lambda a, b: a + b, partitions).collect()
        mode_counts = result.flatMap(lambda r: [((r['short']['dataset'], mode, r['reason']), 1)
                                               for mode in r['modes']]).reduceByKey(
            lambda a, b: a + b, partitions).collect()
        unique_shorts = result.map(lambda r: ((r['short']['dataset'], r['short']['id']),
            frozenset([r['reason']]))).reduceByKey(lambda a, b: a | b, partitions)
        record_counts = unique_shorts.map(lambda r: ((r[0][0], tuple(sorted(r[1]))), 1)).reduceByKey(
            lambda a, b: a + b, partitions).collect()
        selected = result.map(lambda r: ((r['short']['dataset'], r['reason'],
            tuple(r['identity_differences'])), [r])).reduceByKey(attribution_core.bounded, partitions).flatMap(lambda r: r[1])
        run_audit.save(selected.mapPartitions(lambda it: evidence_partition(it, storage, out / 'evidence')),
                       out / 'examples')
        report = {'passed': True, 'diagnostic_only': True, 's3_writes': False,
                  'input_count': count, 'unique_directed_pairs': actual,
                  'pair_reason_counts': [[list(k), v] for k, v in sorted(reason_counts)],
                  'mode_reason_counts': [[list(k), v] for k, v in sorted(mode_counts)],
                  'distinct_short_reason_sets': [[list(k), v] for k, v in sorted(record_counts)],
                  'input_plan_sha256': hashlib.sha256(Path(args.input_plan).read_bytes()).hexdigest(),
                  'elapsed_seconds': round(time.monotonic() - started, 2),
                  'limitations': ['Reason buckets describe candidates, not approved deletions.',
                                  'Different modes can select different witnesses for a short record.',
                                  'Production policy is used only to explain retention after independent detection.']}
        (out / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
        (out / 'SUCCEEDED').write_text('Verified candidates explained against unchanged production policy.\n')
        print(json.dumps({'event': 'completed', 'unique_directed_pairs': actual}), flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
