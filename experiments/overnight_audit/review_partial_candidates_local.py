"""Reproduce a partial-candidate review from an immutable local capture.

The snapshot and source objects must already exist locally. This does not fetch
new objects, submit jobs, alter production files, or prove a final merge count.
"""
import argparse
import collections
import hashlib
import json
import os
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--evidence-root', type=Path, required=True)
    parser.add_argument('--frozen-src', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    assert not args.output.exists()
    summary = json.loads((args.evidence_root / 'snapshot-summary.json').read_text())
    counts = collections.Counter()
    selected, short_ids, long_ids = {}, set(), set()
    digest, total_bytes, incomplete_bytes = hashlib.sha256(), 0, 0
    with (args.evidence_root / 'candidate-prefix.bin').open('rb') as stream:
        for line in stream:
            digest.update(line)
            total_bytes += len(line)
            if not line.endswith(b'\n'):
                incomplete_bytes = len(line)
                break
            row = json.loads(line)
            counts[row['group']] += 1
            short_ids.add(row['short']['id'])
            long_ids.add(row['long']['id'])
            if row['group'] != summary['focus_group']:
                continue
            selected.setdefault('first', row)
            selected['last'] = row
            if counts[row['group']] % 50000 == 0:
                selected['position_' + str(counts[row['group']])] = row
            for side in ('short', 'long'):
                for mode in ('min', 'max'):
                    key = mode + '_' + side + '_bytes'
                    better = key not in selected
                    if not better:
                        a, b = row[side]['bytes'], selected[key][side]['bytes']
                        better = a < b if mode == 'min' else a > b
                    if better:
                        selected[key] = row
    assert total_bytes == summary['captured_prefix_bytes']
    assert digest.hexdigest() == summary['captured_prefix_sha256']
    assert sum(counts.values()) == summary['complete_candidate_lines']
    assert incomplete_bytes == summary['incomplete_final_line_bytes']
    rows = {(r['short']['id'], r['long']['id']): r for r in selected.values()}
    saved = json.loads((args.evidence_root / 'selected-candidates.json').read_text())
    assert set(rows) == {(r['short']['id'], r['long']['id']) for r in saved}

    # Load the unchanged files bundled with the original running job, not the
    # active checkout's later implementation.
    sys.path.insert(0, str(args.frozen_src))
    import trajectory_prefix.mask_job as frozen_job
    from trajectory_prefix.mask_verification import raw_relaxed_covers
    from trajectory_prefix.storage import StorageConfig

    assert Path(frozen_job.__file__).resolve().is_relative_to(args.frozen_src.resolve())
    source_objects = {}
    for row in rows.values():
        for side in ('short', 'long'):
            ref = row[side]
            data = (args.evidence_root / 'sources' / ref['sha256']).read_bytes()
            assert len(data) == ref['bytes'] and hashlib.sha256(data).hexdigest() == ref['sha256']
            source_objects[ref['sha256']] = json.loads(data)
    # Every reference has been verified in this local cache; no S3 access is needed.
    storage = StorageConfig(cache_dir=str(args.evidence_root / 'sources'))
    checks = list(frozen_job.check_partition(saved, storage))
    results = []
    for case, row in enumerate(checks, 1):
        raw = raw_relaxed_covers(source_objects[row['short']['sha256']],
                                 source_objects[row['long']['sha256']],
                                 row['short'], row['long'], frozen_job.POLICY)
        assert raw == row['matched']
        results.append({'case': case, 'short': row['short'], 'long': row['long'],
                        'frozen_pair_match': row['matched'], 'frozen_raw_match': raw,
                        'outside_bound_only': row['outside_bound_only'],
                        'difference_fields': row['fields'], 'recognized_labels': row['labels']})
    source_hashes = {name: hashlib.sha256((args.frozen_src / 'trajectory_prefix' / name).read_bytes()).hexdigest()
                     for name in ('mask_job.py', 'mask_verification.py', 'masking.py',
                                  'projection.py', 'verification.py')}
    report = {'passed': True, 'local_only': True, 's3_accessed': False,
              'partial_candidate_only': True, 'not_final_merge_or_publication_counts': True,
              'complete_candidate_lines_in_snapshot': sum(counts.values()),
              'distinct_short_candidate_ids': len(short_ids), 'distinct_long_candidate_ids': len(long_ids),
              'snapshot_prefix_sha256': digest.hexdigest(), 'incomplete_final_line_bytes': incomplete_bytes,
              'complete_source_objects': len(source_objects), 'reviewed_pairs': len(results),
              'locally_accepted_pairs': sum(r['frozen_pair_match'] for r in results),
              'locally_rejected_pairs': sum(not r['frozen_pair_match'] for r in results),
              'frozen_source_root': str(args.frozen_src), 'frozen_source_sha256': source_hashes,
              'results': results}
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({k: v for k, v in report.items() if k not in ('results', 'frozen_source_sha256')},
                     ensure_ascii=False))


if __name__ == '__main__':
    main()
