"""Diagnose unreplayed reasoning-only envelopes using the frozen full input index."""
import argparse
import hashlib
import json
import os
import time
import zipfile
from pathlib import Path

import audit_core as audit
import run_audit

from trajectory_prefix.storage import StorageConfig

MODE = 'without_empty_visible_assistant'
EMPTY_VISIBLE = audit.digest(audit.dumps({'role': 'assistant', 'content': ''}))


def convert(row):
    vector = row['variants'].get('stable_messages', b'')
    value = b''.join(vector[i:i + 32] for i in range(0, len(vector), 32)
                     if vector[i:i + 32] != EMPTY_VISIBLE)
    return {**row, 'empty_visible_messages': (len(vector) - len(value)) // 32,
            'variants': {MODE: value} if len(value) >= 64 else {}}


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--input-plan', required=True)
    p.add_argument('--input-index', required=True)
    p.add_argument('--expected-count', type=int, required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--partitions', type=int, required=True)
    p.add_argument('--s3-endpoint', required=True)
    p.add_argument('--s3-region', required=True)
    p.add_argument('--s3-credentials-file', required=True)
    args = p.parse_args()
    assert (Path(args.input_index) / '_SUCCESS').is_file()
    from pyspark.sql import SparkSession

    os.umask(0o077)
    spark = SparkSession.builder.appName('independent-reasoning-envelope-probe').getOrCreate()
    sc = spark.sparkContext
    assert sc.master.startswith('k8s://')
    sc.setLogLevel('WARN')
    started = time.monotonic()
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=False)
    storage = StorageConfig(endpoint_url=args.s3_endpoint, region=args.s3_region,
                            credentials_file=args.s3_credentials_file)
    archive = out / 'independent-audit.zip'
    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as z:
        for module in (audit, run_audit):
            z.write(Path(module.__file__), Path(module.__file__).name)
    sc.addPyFile(str(archive))
    try:
        indexed = sc.pickleFile(args.input_index, minPartitions=args.partitions)
        count = indexed.count()
        assert count == args.expected_count
        transformed = indexed.map(convert)
        changed = transformed.filter(lambda r: bool(r['variants'])).count()
        affected = transformed.map(lambda r: (r['ref']['dataset'],
            (int(bool(r['empty_visible_messages'])), r['empty_visible_messages']))).reduceByKey(
                lambda a, b: (a[0] + b[0], a[1] + b[1]), args.partitions).collect()
        ordered = transformed.flatMap(audit.sort_entries).repartitionAndSortWithinPartitions(
            numPartitions=args.partitions, partitionFunc=audit.partition_key)
        run_audit.save(ordered.mapPartitions(audit.neighbors), out / 'candidates')
        candidates = run_audit.lines(sc, out / 'candidates', args.partitions)
        run_audit.save(candidates.repartition(args.partitions).mapPartitions(
            lambda it: run_audit.verify_partition(it, storage)), out / 'verified')
        verified = run_audit.lines(sc, out / 'verified', args.partitions)
        counts = verified.map(lambda r: ((r['short']['dataset'], r['literal_coverage'],
            r['sessionless'], tuple(r['reasoning_only_counts'])), 1)).reduceByKey(
                lambda a, b: a + b, args.partitions).collect()
        assert sum(v for _, v in counts) == candidates.count()
        selected = verified.map(lambda r: ((r['short']['dataset'], r['literal_coverage'],
            r['sessionless'], bool(any(r['reasoning_only_counts']))), [r])).reduceByKey(
                audit.bounded, args.partitions).flatMap(lambda kv: kv[1])
        run_audit.save(selected.mapPartitions(lambda it: run_audit.verify_partition(it, storage, out / 'evidence')),
                       out / 'examples')
        report = {'passed': True, 'input_count': count, 'eligible_after_view': changed,
                  'empty_visible_counts': {k: {'records': v[0], 'messages': v[1]} for k, v in affected},
                  'input_plan_sha256': hashlib.sha256(Path(args.input_plan).read_bytes()).hexdigest(),
                  'mode': MODE, 'diagnostic_only': True, 's3_writes': False,
                  'verified_counts': [[list(k), v] for k, v in counts],
                  'new_candidate_records': sum(v for k, v in counts if not k[1]),
                  'elapsed_seconds': round(time.monotonic() - started, 2)}
        (out / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
        (out / 'SUCCEEDED').write_text('Independent empty-visible-envelope diagnostic completed.\n')
        print(json.dumps(report), flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
