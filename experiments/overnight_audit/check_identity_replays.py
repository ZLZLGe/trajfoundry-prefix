"""Compare visible prefixes, recorded call IDs, timestamps and provenance across known identity boundaries."""

import argparse
import hashlib
import hmac
import os
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path

import audit_core as audit
import orjson

from trajectory_prefix.storage import Reader, StorageConfig


def prefix_features(node, lengths):
    messages, issues = audit.stable_messages(node)
    original = iter(m['tool_calls'] for m in node['messages'] if m['role'] == 'assistant' and m.get('tool_calls'))
    visible, ids = hashlib.sha256(), hashlib.sha256()
    total = missing = 0
    values = {}
    for i, m in enumerate(messages, start=1):
        visible.update(audit.digest(audit.dumps(m)))
        if m['role'] == 'assistant' and m.get('tool_calls'):
            raw_ids = [c.get('id') for c in next(original)]
            total += len(raw_ids)
            missing += sum(not isinstance(v, str) or not v for v in raw_ids)
            ids.update(audit.digest(audit.dumps(raw_ids)))
        if i in lengths:
            values[i] = (visible.digest(), ids.digest(), total, missing)
    assert next(original, None) is None
    return values, len(messages), issues


def worker_start(storage, salt):
    global READER, SALT
    READER, SALT = Reader(storage), salt


def features(jobs):
    output = []
    for ref, lengths in jobs:
        node = READER.node(ref)
        values, count, issues = prefix_features(node, lengths)
        metadata = node.get('metadata') or {}
        stamp = metadata.get('created_at')
        time = datetime.fromisoformat(stamp) if stamp else None
        user = audit.ident(metadata.get('user_id'))
        blind = hmac.digest(SALT, user.encode(), 'sha256') if user else None
        output.append((ref['path'], (values, count, tuple(issues), time.timestamp() if time and time.tzinfo else None,
                                     blind, bool(audit.ident(metadata.get('session_id'))))))
    return output


def targets(source, channel, membership, publication):
    report = orjson.loads((source / 'report.json').read_bytes())
    expected, = [n for (dataset, reason), n in report['pair_reason_counts']
                 if dataset == channel and reason == 'identity_boundary']
    assert (source / 'SUCCEEDED').is_file() and report['passed']
    manifest = (publication / 'published/normalized-manifest.json').read_bytes()
    assert hashlib.sha256(manifest).hexdigest() == membership['target_manifest_sha256']
    available = {r['path']: (r['bytes'], r['sha256']) for r in orjson.loads(manifest)['files']}
    refs, wanted, pairs = {}, defaultdict(set), {}
    for part in sorted((source / 'attributed').glob('part-*')):
        with part.open('rb') as lines:
            for r in map(orjson.loads, lines):
                if r['short']['dataset'] != channel or r['reason'] != 'identity_boundary':
                    continue
                assert r['full_objects_reverified'] and not r['same_scope']
                a, b = r['short']['path'], r['long']['path']
                assert (a, b) not in pairs
                n, m = r['message_counts']
                pairs[a, b] = (tuple(r['identity_differences']), n, m, 'cross_identity' in r['modes'])
                for name in ('short', 'long'):
                    ref = r[name]
                    path = ref['path']
                    assert ref['uri'] == membership['output'] + path
                    assert available[path] == (ref['bytes'], ref['sha256'])
                    refs[path] = ref
                    wanted[path].add(n)
    assert len(pairs) == expected
    return pairs, refs, wanted


def source_sets(publication, membership, refs):
    stage = orjson.loads((publication / 'published/stages/source-inspection.json').read_bytes())
    info = orjson.loads((publication / 'published/report.json').read_bytes())
    assert stage['publication_id'] == info['publication_id']
    location = Path(stage['path'])
    assert (location / '_SUCCESS').is_file()
    identifiers = {}
    for part in sorted(location.glob('part-*')):
        with part.open('rb') as stream:
            for row in map(orjson.loads, stream):
                key = row['path']
                if key in refs and row['status'] == 'kept':
                    assert row['sha256'] == refs[key]['sha256'] and row['bytes'] == refs[key]['bytes']
                    assert key == row['final_path'] and row['trajectory_id'] not in identifiers
                    identifiers[row['trajectory_id']] = key
    assert set(identifiers.values()) == refs.keys()
    digest, results = hashlib.sha256(), {}
    with (publication / 'published/lineage.jsonl').open('rb') as lines:
        for line in lines:
            digest.update(line)
            row = orjson.loads(line)
            path = identifiers.get(row['trajectory_id'])
            if path:
                assert path not in results
                sets = {(o['source_ref'], o['sha256']) for o in row['origins']}
                assert len(sets) == row['origin_count']
                results[path] = sets
    assert results.keys() == refs.keys() and digest.hexdigest() == membership['lineage']['sha256']
    return results


def time_bucket(delta):
    if delta is None:
        return 'missing'
    if delta < 0:
        return 'compared_longer_earlier'
    if delta == 0:
        return 'same_timestamp'
    if delta <= 60:
        return 'later_0_to_60_seconds'
    if delta <= 300:
        return 'later_1_to_5_minutes'
    return 'later_over_5_minutes'


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--attribution', required=True, type=Path)
    p.add_argument('--membership', required=True, type=Path)
    p.add_argument('--publication', required=True, type=Path)
    p.add_argument('--config', required=True, type=Path)
    p.add_argument('--output', required=True, type=Path)
    p.add_argument('--workers', type=int, default=12)
    args = p.parse_args()
    os.umask(0o077)
    member = orjson.loads(args.membership.read_bytes())
    assert member['passed'] and member['all_published_member_references_match']
    pairs, refs, needed = targets(args.attribution, member['channel'], member, args.publication)
    provenance = source_sets(args.publication, member, refs)
    cfg = orjson.loads(args.config.read_bytes())
    storage = StorageConfig(cfg['s3_endpoint'], cfg['s3_region'], cfg['s3_credentials_file'])
    work = [(refs[path], needed[path]) for path in sorted(refs)]
    args.output.mkdir(exist_ok=False)
    records = {}
    with ProcessPoolExecutor(max_workers=args.workers, initializer=worker_start,
                             initargs=(storage, os.urandom(32))) as pool:
        for i, batch in enumerate(pool.map(features, (work[j:j + 64] for j in range(0, len(work), 64))), 1):
            records.update(batch)
            if i % 240 == 0:
                print(f'checked={len(records)}/{len(refs)}', flush=True)
    assert records.keys() == refs.keys()
    counts, time_counts, strict_users, graph = defaultdict(Counter), defaultdict(Counter), set(), Counter()
    with (args.output / 'checks.jsonl').open('wb') as out:
        for (a, b), (diff, n, m, cross_mode) in pairs.items():
            av, actual_a, ai, at, au, ases = records[a]
            bv, actual_b, bi, bt, bu, bses = records[b]
            assert (n, m) == (actual_a, actual_b) and n <= m and n in av and n in bv
            ah, aid, ac, am = av[n]
            bh, bid, bc, bm = bv[n]
            stable = ah == bh
            if cross_mode:
                assert stable
            same_ids = bool(ac) and ac == bc and am == bm == 0 and aid == bid
            overlap = len(provenance[a] & provenance[b])
            delta = round(bt - at, 6) if at is not None and bt is not None else None
            bucket = time_bucket(delta)
            name = 'only_user_id' if diff == ('user',) else 'other_identity_boundary'
            groups = ['all', name]
            if n < m and name == 'only_user_id':
                groups.append('strict_only_user_id')
                strict_users.add(a)
                if au and bu:
                    assert au != bu
                    graph[au, bu] += 1
            for group in groups:
                c = counts[group]
                c['pairs'] += 1
                c['strict'] += n < m
                c['visible_full_prefix'] += stable
                c['both_sessionless'] += not ases and not bses
                c['tool_calls_on_short'] += bool(ac)
                c['same_recorded_ids_on_prefix'] += same_ids
                c['distinct_source_sets_with_no_overlap'] += not overlap
                c['overlapping_sources'] += bool(overlap)
                c['binding_warning_on_either'] += bool(ai or bi)
                c['visible_prefix_same_ids_later_60s'] += bool(n < m and stable and same_ids and not ai and not bi
                                                              and delta is not None and 0 < delta <= 60)
                time_counts[group][bucket] += 1
            out.write(orjson.dumps({'short': a, 'long': b, 'identity_fields_differ': diff,
                                    'message_counts': [n, m], 'stable_prefix': stable, 'short_calls': ac,
                                    'same_recorded_ids_on_prefix': same_ids, 'shared_raw_origins': overlap,
                                    'representative_time_gap_seconds': delta}) + b'\n')
    result = {'passed': True, 'uri': member['output'], 'manifest_sha256': member['target_manifest_sha256'],
              'lineage_sha256': member['lineage']['sha256'], 'source_files_read': len(refs),
              'verified_source_bytes': sum(r['bytes'] for r in refs.values()), 'counts': counts, 'time_buckets': time_counts,
              'distinct_strict_shorter_only_user': len(strict_users), 'distinct_directed_nonempty_user_pairs': len(graph),
              'pairs_accounted_by_user_pair_frequencies': sum(graph.values()),
              'top10_directed_user_pair_frequencies': [n for _, n in graph.most_common(10)],
              'scope': 'Known independently detected cross-identity pairs from one published channel and date. '
                       'Visible matches use the stable projection; tool IDs use their actual normalized values. '
                       'Time uses normalized metadata.created_at. Different captures and matching IDs do not '
                       'identify a real actor or authorize a cross-user merge.'}
    (args.output / 'report.json').write_bytes(orjson.dumps(result, option=orjson.OPT_INDENT_2) + b'\n')
    (args.output / 'SUCCEEDED').write_text('Known candidates re-read and joined to full verified source lineage.\n')
    print(orjson.dumps({k: v for k, v in result.items() if k != 'scope'}).decode())


if __name__ == '__main__':
    main()
