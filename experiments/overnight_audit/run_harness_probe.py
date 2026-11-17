"""Local diagnostic: exact visible prefixes separated only by harness labels.

All other identity fields are kept. One full-object witness per covered record
is enough for this diagnostic; candidates never authorize deletion.
"""
import argparse
import itertools
import json
import os
import time
import zipfile
from pathlib import Path

import audit_core as audit
import local_runtime
import run_audit

from trajectory_prefix.projection import Policy, covers, project
from trajectory_prefix.storage import Reader, StorageConfig, atomic_bytes


def group_key(row):
    values = row['scope_values']
    return audit.digest(audit.dumps(values[:2] + values[3:]))


def entry(row):
    vector = row['variants']['stable_messages']
    return (group_key(row), vector, row['ref']['id']), {
        k: row[k] for k in ['ref', 'harness', 'scope_values']
    }


def partition_key(key):
    return int.from_bytes(audit.digest(key[0] + key[1][:32])[:8], 'big')


def candidates(rows):
    """Find one other-harness witness, including nonadjacent extensions.

    A prefix stack holds only records still lacking a witness. Equal-vector
    blocks are handled together so same-harness neighbors cannot hide a match.
    """
    scope, stack = None, []
    for (current_scope, vector), block in itertools.groupby(rows, key=lambda x: x[0][:2]):
        if current_scope != scope:
            scope, stack = current_scope, []
        while stack and not vector.startswith(stack[-1][0]):
            stack.pop()
        values = [value for _, value in block]
        witnesses = {}
        for value in values:
            witnesses.setdefault(value['harness'], value)

        def witness(harness):
            return next((v for h, v in witnesses.items() if h != harness), None)

        for _, pending in stack:
            for harness in list(pending):
                longer = witness(harness)
                if longer is None:
                    continue
                for shorter in pending.pop(harness):
                    yield {'short': shorter['ref'], 'long': longer['ref'],
                           'harnesses': [harness, longer['harness']], 'relation': 'prefix'}
        pending = {}
        for value in values:
            longer = witness(value['harness'])
            if longer is not None:
                yield {'short': value['ref'], 'long': longer['ref'],
                       'harnesses': [value['harness'], longer['harness']], 'relation': 'equal'}
            else:
                pending.setdefault(value['harness'], []).append(value)
        stack.append((vector, pending))


def verify(rows, storage, evidence=None):
    reader = Reader(storage)
    policy = Policy(extra_information='messages_only')
    for row in rows:
        raw = [reader.read(row[side]) for side in ['short', 'long']]
        nodes = [json.loads(value) for value in raw]
        scopes = [audit.scope(node, row[side]) for node, side in zip(nodes, ['short', 'long'])]
        assert scopes[0][2] != scopes[1][2]
        assert scopes[0][:2] + scopes[0][3:] == scopes[1][:2] + scopes[1][3:]
        messages = [audit.stable_messages(node)[0] for node in nodes]
        # Canonical JSON retains the distinction between boolean and numeric values.
        assert audit.is_prefix(*[list(map(audit.dumps, value)) for value in messages])
        projections = [project(node, row[side]['dataset'], row[side]['date'], policy)
                       for node, side in zip(nodes, ['short', 'long'])]
        assert not covers(*projections)
        if evidence:
            for side, value in zip(['short', 'long'], raw):
                ref = row[side]
                atomic_bytes(Path(evidence) / (ref['sha256'] + '.json'), value, ref['sha256'])
        yield {**row, 'full_objects_verified': True, 'diagnostic_only': True,
               'model': nodes[0].get('model', ''), 'message_counts': list(map(len, messages)),
               'sessionless': scopes[0][-2] == 'sessionless',
               'isolation_reasons': sorted({p.isolation_reason for p in projections if not p.mergeable}),
               'visible_chars': sum(len(m['content']) for m in messages[0]
                                    if m['role'] in {'user', 'assistant', 'tool'}),
               'has_tool_result': any(m['role'] == 'tool' for m in messages[0]),
               'production_covers': False}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-index', type=Path, required=True)
    parser.add_argument('--expected-count', type=int, required=True)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    assert (args.input_index / '_SUCCESS').is_file()
    os.umask(0o077)
    args.output.mkdir(parents=True, exist_ok=False)
    config = json.loads(args.config.read_text())
    storage = StorageConfig(endpoint_url=config['s3_endpoint'], region=config['s3_region'],
                            credentials_file=config['s3_credentials_file'])
    spark = local_runtime.spark_session('local-harness-boundary-probe', 8)
    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    assert sc.master == 'local[8]'
    start = time.monotonic()
    archive = args.output / 'harness-probe.zip'
    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as target:
        for path in [Path(__file__), Path(audit.__file__), Path(run_audit.__file__), Path(local_runtime.__file__)]:
            target.write(path, path.name)
    sc.addPyFile(str(archive))
    partitions = 256
    try:
        indexed = sc.pickleFile(str(args.input_index), minPartitions=partitions)
        count = indexed.count()
        assert count == args.expected_count
        harness_counts = indexed.map(lambda r: ((r['ref']['dataset'], r['harness']), 1)).reduceByKey(
            lambda a, b: a + b, partitions).collect()
        keyed = indexed.filter(lambda r: 'stable_messages' in r['variants']).map(lambda r: (group_key(r), r))
        mixed = keyed.mapValues(lambda r: frozenset([r['harness']])).reduceByKey(
            lambda a, b: a | b, partitions).filter(lambda r: len(r[1]) > 1)
        groups = mixed.count()
        print(json.dumps({'event': 'harness_census', 'mixed_identity_groups': groups,
                          'harness_counts': [[list(k), v] for k, v in sorted(harness_counts)]}), flush=True)
        selected = keyed.join(mixed, partitions).map(lambda r: r[1][0])
        sorted_rows = selected.map(entry).repartitionAndSortWithinPartitions(
            numPartitions=partitions, partitionFunc=partition_key)
        run_audit.save(sorted_rows.mapPartitions(candidates), args.output / 'candidates')
        proposed = run_audit.lines(sc, args.output / 'candidates', partitions)
        run_audit.save(proposed.repartition(partitions).mapPartitions(lambda it: verify(it, storage)),
                       args.output / 'verified')
        verified = run_audit.lines(sc, args.output / 'verified', partitions)
        candidate_count = verified.count()
        assert candidate_count == proposed.count()
        assert verified.map(lambda r: r['short']['id']).distinct(partitions).count() == candidate_count
        counts = verified.map(lambda r: ((r['short']['dataset'], tuple(r['harnesses']),
            r['sessionless'], tuple(r['isolation_reasons'])), 1)).reduceByKey(
                lambda a, b: a + b, partitions).collect()
        examples = verified.map(lambda r: ((r['short']['dataset'], tuple(r['harnesses']),
            r['sessionless'], bool(r['isolation_reasons'])), [r | {'mode': 'cross_harness'}])).reduceByKey(
                audit.bounded, partitions).flatMap(lambda r: r[1])
        run_audit.save(examples.mapPartitions(lambda it: verify(it, storage, args.output / 'sources')),
                       args.output / 'examples')
        report = {'passed': True, 'diagnostic_only': True, 's3_writes': False,
                  'spark_master': sc.master, 'input_count': count, 'candidate_pairs': candidate_count,
                  'distinct_short_records': candidate_count, 'mixed_identity_groups': groups,
                  'harness_counts': [[list(k), v] for k, v in sorted(harness_counts)],
                  'verified_counts': [[list(k), v] for k, v in sorted(counts)],
                  'elapsed_seconds': round(time.monotonic() - start, 2),
                  'limitation': 'Different harnesses may represent different clients or agents. Equal pairs can be bidirectional; counts are not removals.'}
        (args.output / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
        (args.output / 'SUCCEEDED').write_text('All harness-boundary candidates verified locally.\n')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
