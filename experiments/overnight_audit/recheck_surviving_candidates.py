"""Independently verify initial mask candidates whose two endpoints remain in the retained plan."""

import argparse
import hashlib
import os
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from functools import lru_cache
from pathlib import Path

import orjson

from trajectory_prefix.mask_verification import raw_relaxed_covers
from trajectory_prefix.projection import Policy
from trajectory_prefix.storage import Reader, StorageConfig, file_sha256


def parts(directory):
    assert (directory / '_SUCCESS').is_file()
    for p in sorted(directory.glob('part-*')):
        with p.open('rb') as src:
            yield from map(orjson.loads, src)


def signature(row):
    values = [row[k] for k in ('short', 'long', 'group', 'different_messages')]
    return hashlib.sha256(orjson.dumps(values, option=orjson.OPT_SORT_KEYS)).digest()


def retained_records(refined):
    check = orjson.loads((refined / 'ordinary-final-recheck/report.json').read_bytes())
    plan = refined / 'final-input.jsonl'
    assert check['check_only'] and check['removed_count'] == 0 and (refined / 'ordinary-final-recheck/SUCCEEDED').is_file()
    digest, keep = hashlib.sha256(), {}
    with plan.open('rb') as lines:
        for line in lines:
            digest.update(line)
            r = orjson.loads(line)
            assert r['id'] not in keep
            keep[r['id']] = (r['bytes'], r['sha256'], r['uri'])
    assert digest.hexdigest() == check['input_plan_sha256'] and len(keep) == check['kept_count']
    return keep, digest.hexdigest()


def survivors(refined, keep):
    pending, counts, subset = set(), Counter(), []
    for row in parts(refined / 'main-candidates'):
        key = signature(row)
        assert key not in pending
        pending.add(key)
    for row in parts(refined / 'main-checks'):
        pending.remove(signature(row))
        endpoints = 0
        for side in ('short', 'long'):
            ref = row[side]
            if ref['id'] in keep:
                assert keep[ref['id']] == (ref['bytes'], ref['sha256'], ref['uri'])
                endpoints += 1
        count_type = 'matched' if row['matched'] else 'rejected'
        counts[f'{endpoints}_retained_{count_type}'] += 1
        if endpoints == 2:
            subset.append(row)
    assert not pending
    return subset, counts


def start_reader(storage):
    global READER
    READER = Reader(storage)


@lru_cache(maxsize=16)
def node(uri, sha, size):
    return READER.node({'id': sha, 'uri': uri, 'sha256': sha, 'bytes': size})


def verify(row):
    ar, br = row['short'], row['long']
    a, b = (node(r['uri'], r['sha256'], r['bytes']) for r in (ar, br))
    accepted = raw_relaxed_covers(a, b, ar, br, Policy(extra_information='messages_only'))
    return {'short': ar['path'], 'long': br['path'], 'initial_matched': row['matched'],
            'independent_raw_match': accepted, 'initial_outside_bound_only': row['outside_bound_only'],
            'different_messages': row['different_messages'], 'group': row['group'],
            'candidate_sha256': signature(row).hex()}


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--refined', type=Path, required=True)
    p.add_argument('--config', type=Path, required=True)
    p.add_argument('--frozen-source', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--workers', type=int, default=6)
    args = p.parse_args()
    os.umask(0o077)
    import trajectory_prefix.mask_verification as oracle
    implementation = Path(oracle.__file__).resolve()
    assert implementation == (args.frozen_source / 'trajectory_prefix/mask_verification.py').resolve()
    keep, digest = retained_records(args.refined)
    n = len(keep)
    rows, counts = survivors(args.refined, keep)
    del keep
    unique = {r[k]['id']: r[k] for r in rows for k in ('short', 'long')}
    args.output.mkdir(exist_ok=False)
    cfg = orjson.loads(args.config.read_bytes())
    storage = StorageConfig(cfg['s3_endpoint'], cfg['s3_region'], cfg['s3_credentials_file'], str(args.output / 'sources'))
    order = sorted(rows, key=lambda r: (r['group'], r['short']['id'], r['long']['id']))
    results = Counter()
    with (ProcessPoolExecutor(max_workers=args.workers, initializer=start_reader, initargs=(storage,)) as pool,
          (args.output / 'pair-checks.jsonl').open('wb') as dst):
        for v in pool.map(verify, order, chunksize=48):
            dst.write(orjson.dumps(v) + b'\n')
            results['checked'] += 1
            results['raw_matches'] += v['independent_raw_match']
            results['disagreements'] += v['initial_matched'] != v['independent_raw_match']
            results['initial_outside_bound_only'] += v['initial_outside_bound_only']
    assert len(rows) == results['checked']
    result = {'passed': True, 'local_only': True, 'plan_sha256': digest, 'ordinary_rechecked_records': n,
              'initial_candidates_checked': sum(counts.values()), 'initial_partition': counts,
              'replayed': results, 'unique_source_files_checked': len(unique),
              'unique_source_bytes_checked': sum(ref['bytes'] for ref in unique.values()),
              'frozen_oracle_sha256': file_sha256(implementation),
              'scope': 'Complete initial candidate set joined to the recorded preliminary retained inventory. '
                       'Both complete original normalized files are checked for every surviving pair. '
                       'The fresh cluster candidate search, its completion and final publication are separate.'}
    (args.output / 'report.json').write_bytes(orjson.dumps(result, option=orjson.OPT_INDENT_2) + b'\n')
    (args.output / 'SUCCEEDED').write_text('Initial candidates and their retained endpoints reconciled.\n')
    print(orjson.dumps(result).decode())


if __name__ == '__main__':
    main()
