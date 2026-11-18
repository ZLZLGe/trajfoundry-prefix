"""Read both original objects for every candidate from the local sessionless cwd scan."""

import argparse
import hashlib
import os
import threading
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from pathlib import Path

import audit_core as audit
import orjson
from explain_declared_cwd import declarations, relation
from survey_explicit_cwd import CD, projection

from trajectory_prefix.storage import Reader, StorageConfig


def candidate_pairs(scan):
    shorts, pairs, refs = defaultdict(list), set(), {}
    with (scan / 'possible-shorts.jsonl').open('rb') as stream:
        for row in map(orjson.loads, stream):
            shorts[row['key']].append(row)
    for part in sorted((scan / 'longs').glob('part-*')):
        with part.open('rb') as stream:
            for long in map(orjson.loads, stream):
                for short in shorts[long['key']]:
                    if short['terminal_hash'] == long['message_hash']:
                        continue
                    for row in (short, long):
                        ref = row['ref']
                        assert ref['path'].startswith('no_session_id_')
                        known = refs.setdefault(ref['id'], ref)
                        assert known == ref
                    pair = short['ref']['id'], long['ref']['id']
                    assert pair not in pairs
                    pairs.add(pair)
    return pairs, refs


def evidence(node, ref):
    messages, issues = audit.stable_messages(node)
    original = iter(m['tool_calls'] for m in node['messages'] if m['role'] == 'assistant' and m.get('tool_calls'))
    calls = {}
    for i, m in enumerate(messages):
        if m['role'] == 'assistant' and m.get('tool_calls'):
            calls[i] = tuple(c.get('id') for c in next(original))
    assert next(original, None) is None
    hashes = tuple(audit.digest(audit.dumps(m)) for m in messages)
    return audit.scope(node, ref), messages, hashes, calls, issues


def check_pair(left, right):
    ls, lm, lh, lc, li = left
    rs, rm, rh, rc, ri = right
    if ls[-2] != 'sessionless' or rs[-2] != 'sessionless':
        return 'has_session_id', None
    if ls != rs:
        return 'different_identity_scope', None
    pos = len(lm) - 1
    if pos < 0 or len(lm) >= len(rm) or lh[:pos] != rh[:pos]:
        return 'different_earlier_history_or_length', None
    if pos not in lc or pos not in rc:
        return 'missing_assistant_tool_call', None
    stripped, prefixes = projection(lm[pos])
    if not prefixes:
        return 'no_literal_absolute_bash_cd', None
    # In a batch of tool calls, other commands may retain their explicit cd.
    # Only permit changes from a short-side literal cd to exactly its suffix.
    calls = list(lm[pos]['tool_calls'])
    altered, ids, paths = [], [], set()
    for i, (a, b) in enumerate(zip(calls, rm[pos]['tool_calls'])):
        if a == b:
            continue
        if i not in prefixes or stripped['tool_calls'][i] != b:
            return 'different_command_or_arguments', None
        calls[i] = stripped['tool_calls'][i]
        altered.append(i)
        ids.append(bool(lc[pos][i]) and lc[pos][i] == rc[pos][i])
        match = CD.match(prefixes[i])
        paths.add(next(p for p in match.groups() if p is not None))
    if not altered or {**lm[pos], 'tool_calls': calls} != rm[pos]:
        return 'other_terminal_difference', None
    return 'verified', {'changed_call_positions': altered, 'same_original_ids': all(ids),
                        'lengths': [len(lm), len(rm)], 'binding_issues': sorted(set(li + ri)),
                        'prior_declared_directory': relation(paths, declarations(lm[:pos])[-1])}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--scan', required=True, type=Path)
    parser.add_argument('--config', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    scan = orjson.loads((args.scan / 'report.json').read_bytes())
    assert scan['passed'] and (args.scan / 'SUCCEEDED').is_file() and (args.scan / 'longs/_SUCCESS').is_file()
    pairs, refs = candidate_pairs(args.scan)
    args.output.mkdir(exist_ok=False)
    config = orjson.loads(args.config.read_bytes())
    cache = args.output / 'sources'
    storage = StorageConfig(config['s3_endpoint'], config['s3_region'], config['s3_credentials_file'], str(cache))
    workers = threading.local()

    def fetch(ref):
        if not hasattr(workers, 'reader'):
            workers.reader = Reader(storage)
        workers.reader.read(ref)

    with ThreadPoolExecutor(max_workers=6) as pool:
        for _ in pool.map(fetch, refs.values()):
            pass

    @lru_cache(maxsize=32)
    def read(key):
        ref = refs[key]
        data = (cache / ref['sha256']).read_bytes()
        assert len(data) == ref['bytes'] and hashlib.sha256(data).hexdigest() == ref['sha256']
        return evidence(orjson.loads(data), ref)

    counts, destinations, id_matches = Counter(), defaultdict(set), defaultdict(set)
    statuses, scopes = Counter(), Counter()
    with (args.output / 'pairs.jsonl').open('wb') as out:
        for a, b in sorted(pairs):
            left, right = read(a), read(b)
            reason, detail = check_pair(left, right)
            counts[reason] += 1
            if detail:
                destinations[a].add(b)
                id_matches[a].add(detail['same_original_ids'])
                statuses['identical_original_ids'] += detail['same_original_ids']
                statuses['binding_issues'] += bool(detail['binding_issues'])
                scopes[left[0][1]] += 1
            out.write(orjson.dumps({'short': a, 'long': b, 'reason': reason, 'detail': detail}) + b'\n')
    all_shorts = set(destinations)
    report = {'passed': True, 'plan_sha256': scan['plan_sha256'], 'scan_counts': scan['counts'],
              'candidate_pairs': len(pairs), 'candidate_shorts': len({a for a, _ in pairs}),
              'unique_files_read': len(refs), 'verified_source_bytes': sum(r['bytes'] for r in refs.values()),
              'pair_results': counts, 'verified_shorts': len(all_shorts),
              'verified_longs': len(set().union(*destinations.values())),
              'shorts_with_a_long_outside_short_set': sum(bool(bs - all_shorts) for bs in destinations.values()),
              'shorts_with_identical_original_ids': sum(True in flags for flags in id_matches.values()),
              'verified_pair_attributes': statuses, 'verified_pairs_by_model': scopes,
              'limit': 'Visible histories under the independent projection. Neither the actual execution directory '
                       'nor common real-world session identity is proved by text alone.'}
    assert counts.total() == len(pairs)
    (args.output / 'report.json').write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2) + b'\n')
    (args.output / 'SUCCEEDED').write_text('All candidate pairs checked against verified original sources.\n')
    print(orjson.dumps(report).decode())


if __name__ == '__main__':
    main()
