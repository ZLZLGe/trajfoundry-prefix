"""Cluster-only full retained-set diagnostics; writes only to the shared run dir."""
import argparse
import hashlib
import json
import os
import time
import zipfile
from pathlib import Path

import audit_core as audit
import local_runtime

from trajectory_prefix.storage import Reader, StorageConfig, atomic_bytes, reference


def lines(sc, path, partitions):
    return sc.textFile(str(path), minPartitions=partitions).filter(bool).map(json.loads)


def save(rdd, path):
    rdd.map(lambda r: audit.dumps(r).decode()).saveAsTextFile(str(path))


def index_partition(entries, storage):
    reader = Reader(storage)
    for entry in entries:
        ref = reference(json.loads(entry))
        yield audit.index_node(reader.node(ref), ref)


def verify_partition(entries, storage, evidence_dir=None):
    os.umask(0o077)
    reader = Reader(storage)
    for row in entries:
        left_data, right_data = reader.read(row['short']), reader.read(row['long'])
        left, right = json.loads(left_data), json.loads(right_data)
        verified = audit.verify(row, left, right)
        if evidence_dir:
            sources = Path(evidence_dir) / 'sources'
            sources.mkdir(parents=True, exist_ok=True)
            for ref, data in ((row['short'], left_data), (row['long'], right_data)):
                target = sources / (ref['sha256'] + '.json')
                atomic_bytes(target, data, ref['sha256'])
            common = verified['common_stable_messages']
            verified['contexts'] = []
            for node in (left, right):
                messages, _ = audit.stable_messages(node)
                indexes = sorted({0, 1, max(0, common - 1), common, common + 1, common + 2, len(messages) - 1})
                verified['contexts'].append([
                    {'message_index': i, 'role': messages[i]['role'],
                     'content': audit.safe_excerpt(messages[i]['content'], 5000),
                     'tool_calls': audit.safe_excerpt(messages[i].get('tool_calls', []), 5000)}
                    for i in indexes if i < len(messages)
                ])
        yield verified


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--input-plan', required=True)
    p.add_argument('--expected-count', type=int, required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--partitions', type=int, default=512)
    p.add_argument('--local-workers', type=int, default=0)
    p.add_argument('--s3-endpoint', required=True)
    p.add_argument('--s3-region', required=True)
    p.add_argument('--s3-credentials-file', required=True)
    args = p.parse_args()
    os.umask(0o077)
    spark = local_runtime.spark_session('independent-retained-prefix-audit', args.local_workers)
    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()

    def event(name, **values):
        data = {'event': name, 'elapsed_seconds': round(time.monotonic() - started, 2), **values}
        with (out / 'progress.jsonl').open('a') as stream:
            stream.write(json.dumps(data) + '\n')
        print(json.dumps(data), flush=True)

    storage = StorageConfig(endpoint_url=args.s3_endpoint, region=args.s3_region,
                            credentials_file=args.s3_credentials_file)
    archive = out / 'independent-audit.zip'
    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as z:
        z.write(Path(audit.__file__), 'audit_core.py')
        z.write(Path(local_runtime.__file__), 'local_runtime.py')
    sc.addPyFile(str(archive))
    partitions = args.partitions
    try:
        event('index_started', expected_count=args.expected_count)
        sc.textFile(args.input_plan, minPartitions=partitions).filter(bool).mapPartitions(
            lambda it: index_partition(it, storage)).saveAsPickleFile(str(out / 'index'))
        indexed = sc.pickleFile(str(out / 'index'), minPartitions=partitions)
        count = indexed.count()
        assert count == args.expected_count
        census = indexed.mapPartitions(audit.census).reduceByKey(lambda a, b: a + b, partitions).collect()
        (out / 'census.json').write_text(json.dumps([[list(k), v] for k, v in census], ensure_ascii=False, indent=2) + '\n')
        event('index_complete', count=count)
        family = indexed.map(lambda r: ((r['ref']['dataset'], r['family']), (1, r['sub_number']))).reduceByKey(
            lambda a, b: (a[0] + b[0], max(a[1], b[1])), partitions)
        top_families = family.takeOrdered(30, key=lambda r: (-r[1][0], -r[1][1], r[0]))
        (out / 'largest-families.json').write_text(json.dumps(top_families, ensure_ascii=False, indent=2) + '\n')
        sorted_rows = indexed.flatMap(audit.sort_entries).repartitionAndSortWithinPartitions(
            numPartitions=partitions, partitionFunc=audit.partition_key)
        save(sorted_rows.mapPartitions(audit.neighbors), out / 'neighbors')
        neighbors = lines(sc, out / 'neighbors', partitions)
        counts = neighbors.map(lambda r: ((r['short']['dataset'], r['kind'], r['mode']), 1)).reduceByKey(
            lambda a, b: a + b, partitions).collect()
        event('candidate_scan_complete', counts=[[list(k), v] for k, v in counts])
        coverage = neighbors.filter(lambda r: r['kind'] == 'coverage')
        save(coverage.repartition(partitions).mapPartitions(lambda it: verify_partition(it, storage)),
             out / 'verified-coverage')
        verified = lines(sc, out / 'verified-coverage', partitions)
        verified_counts = verified.map(lambda r: ((r['short']['dataset'], r['mode'], r['same_identity']), 1)).reduceByKey(
            lambda a, b: a + b, partitions).collect()
        expected = sum(n for key, n in counts if key[1] == 'coverage')
        assert sum(n for _, n in verified_counts) == expected
        unique = verified.map(lambda r: (r['short']['dataset'], r['short']['id'])).distinct(partitions).map(
            lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b, partitions).collect()
        event('all_coverage_verified', candidates=expected, counts=[[list(k), v] for k, v in verified_counts])
        selected_coverage = verified.map(lambda r: ((r['short']['dataset'], r['mode'],
            r['sessionless'], r['same_identity'], bool(any(r['binding_issues']))), [r])).reduceByKey(
                audit.bounded, partitions).flatMap(lambda kv: kv[1])
        save(selected_coverage.mapPartitions(lambda it: verify_partition(it, storage, out / 'evidence')),
             out / 'coverage-examples')
        branches = neighbors.filter(lambda r: r['kind'] == 'branch_neighbor')
        selected_branches = branches.map(lambda r: ((r['short']['dataset'], r['first_differing_roles'],
            r['sessionless'], r['common_messages'] >= 6, r['max_sub_number'] >= 1000), [r])).reduceByKey(
                audit.bounded, partitions).flatMap(lambda kv: kv[1])
        save(selected_branches.mapPartitions(lambda it: verify_partition(it, storage, out / 'evidence')),
             out / 'branch-examples')
        branch_count = lines(sc, out / 'branch-examples', partitions).count()
        examples_count = lines(sc, out / 'coverage-examples', partitions).count()
        report = {
            'passed': True, 'diagnostic_only': True, 's3_writes': False,
            'input_count': count, 'input_plan_sha256': hashlib.sha256(Path(args.input_plan).read_bytes()).hexdigest(),
            'modes': list(audit.MODES), 'neighbor_counts': [[list(k), v] for k, v in counts],
            'verified_coverage_counts': [[list(k), v] for k, v in verified_counts],
            'unique_short_records_across_views': dict(unique), 'branch_examples': branch_count,
            'coverage_examples': examples_count, 'spark_master': sc.master,
            'elapsed_seconds': round(time.monotonic() - started, 2),
            'limitations': ['Diagnostic candidates are not approved removals.',
                            'Cross-identity common history does not prove the same real session.',
                            'Branch-neighbor selection is not an exhaustive all-pairs semantic audit.'],
        }
        (out / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
        (out / 'SUCCEEDED').write_text('Complete retained input and full raw coverage verification passed.\n')
        event('completed', **{k: report[k] for k in ['input_count', 'branch_examples', 'coverage_examples']})
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
